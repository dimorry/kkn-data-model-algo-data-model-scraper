import logging
import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

import duckdb
import pandas as pd


class EtnCdmMappingUpserter:
    """Generate and load ETN CDM mapping records by reconciling KNX metadata with ETN mappings."""

    SAP_TABLE_HINTS: Dict[str, Tuple[str, str]] = {
        'kna1': ('XD03', 'Display Customer (General Data)'),
        'knvv': ('XD03', 'Display Customer (Sales Area Data)'),
        'mara': ('MM03', 'Display Material'),
        'marc': ('MM03', 'Display Material - Plant Data'),
        'mbew': ('MM03', 'Display Material Valuation'),
        'makt': ('MM03', 'Display Material Description'),
        'tvkot': ('VK01', 'Maintain Sales Document Types'),
        't438m': ('MD04', 'Display Stock/Requirements List'),
    }

    def __init__(self, db_path: str = "mappings.duckdb", logger: Optional[logging.Logger] = None):
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(self, con: Optional[duckdb.DuckDBPyConnection] = None) -> None:
        manage_connection = False

        if con is None:
            con = duckdb.connect(self.db_path)
            manage_connection = True

        try:
            self.logger.info("Starting ETN CDM mapping upsert process")

            data = self._load_source_data(con)
            matched_rows = self._match_records(data)
            assembled_rows = self._assemble_rows(matched_rows)

            self._persist_rows(con, assembled_rows)
            self.logger.info("Completed ETN CDM mapping upsert with %d rows", len(assembled_rows))

        except Exception as exc:
            self.logger.error("Failed ETN CDM mapping upsert: %s", exc)
            raise
        finally:
            if manage_connection:
                con.close()

    # ------------------------------------------------------------------
    # Data Loading
    # ------------------------------------------------------------------
    def _load_source_data(self, con: duckdb.DuckDBPyConnection) -> Dict[str, pd.DataFrame]:
        self.logger.debug("Loading KNX and ETN source data")

        knx_extended = con.execute(
            """
            SELECT
                id,
                table_id,
                table_name,
                field_name,
                description,
                data_type,
                is_key,
                is_calculated,
                referenced_table,
                is_extended,
                display_on_export,
                created_at
            FROM knx_doc_extended
        """
        ).fetchdf()

        tables_df = con.execute(
            """
            SELECT id, name, description
            FROM knx_doc_tables
        """
        ).fetchdf()
        tables_df = tables_df.rename(columns={'description': 'table_description'})

        columns_df = con.execute(
            """
            SELECT
                id,
                table_id,
                field_name,
                description,
                data_type,
                is_key,
                display_on_export
            FROM knx_doc_columns
        """
        ).fetchdf()

        etn_mappings = con.execute(
            """
            SELECT
                m.id,
                m.knx_table,
                m.target_field,
                m.source_table,
                m.source_field,
                m.special_extract_logic,
                m.transformation_table_name,
                m.constant_value,
                m.example_value,
                m.notes,
                m.show_output,
                m.sort_output,
                t.domain AS trl_domain
            FROM etn_doc_mappings AS m
            LEFT JOIN trl_cdm_augmentation AS t
                ON lower(trim(m.knx_table)) = lower(trim(t.entity))
        """
        ).fetchdf()

        knx_extended['field_name_trim'] = knx_extended['field_name'].astype(str).str.strip()
        knx_extended['data_type_lower'] = knx_extended['data_type'].astype(str).str.lower()
        knx_extended = knx_extended[~knx_extended['data_type_lower'].str.startswith('reference', na=False)].copy()

        knx_extended = knx_extended.merge(
            tables_df[['id', 'table_description']],
            left_on='table_id',
            right_on='id',
            how='left'
        ).drop(columns=['id_y']).rename(columns={'id_x': 'id'})

        columns_df['field_name_trim'] = columns_df['field_name'].astype(str).str.strip()
        columns_df = columns_df.rename(columns={
            'id': 'knx_column_id',
            'field_name': 'knx_column_field_name',
            'description': 'knx_column_description',
            'data_type': 'knx_column_data_type',
            'is_key': 'knx_column_is_key',
        })

        knx_extended = knx_extended.merge(
            columns_df[['table_id', 'field_name_trim', 'knx_column_id', 'knx_column_field_name',
                        'knx_column_description', 'knx_column_data_type', 'knx_column_is_key']],
            left_on=['table_id', 'field_name_trim'],
            right_on=['table_id', 'field_name_trim'],
            how='left'
        )

        etn_mappings['target_field_trim'] = etn_mappings['target_field'].astype(str).str.strip()

        return {
            'knx_extended': knx_extended,
            'etn_mappings': etn_mappings,
        }

    # ------------------------------------------------------------------
    # Matching Logic
    # ------------------------------------------------------------------
    def _match_records(self, data: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        knx_df = data['knx_extended']
        etn_df = data['etn_mappings']

        match_payloads: List[Dict[str, Any]] = []

        tables = sorted(set(knx_df['table_name'].dropna().unique()).union(
            set(etn_df['knx_table'].dropna().unique())
        ))

        for table_name in tables:
            table_knx = knx_df[knx_df['table_name'] == table_name].copy()
            table_etn = etn_df[etn_df['knx_table'] == table_name].copy()

            knx_records = [self._prepare_knx_record(row) for _, row in table_knx.iterrows()]
            etn_records = [self._prepare_etn_record(row) for _, row in table_etn.iterrows()]

            matches = self._reconcile_table(table_name, knx_records, etn_records)
            match_payloads.extend(matches)

        return match_payloads

    def _prepare_knx_record(self, row: pd.Series) -> Dict[str, Any]:
        field_name_trim = str(row.get('field_name_trim', '') or '').strip()
        tokens, token_bag = self._tokenize(field_name_trim)

        return {
            'source': 'KNX',
            'raw': row,
            'table_name': row.get('table_name'),
            'field_name_trim': field_name_trim,
            'normalized_tokens': tokens,
            'normalized_keys': self._generate_keys(tokens),
            'token_bag': token_bag,
            'table_description': row.get('table_description'),
            'knx_column_field_name': row.get('knx_column_field_name'),
            'knx_column_description': row.get('knx_column_description'),
            'knx_column_data_type': row.get('knx_column_data_type'),
            'knx_column_is_key': row.get('knx_column_is_key'),
        }

    def _prepare_etn_record(self, row: pd.Series) -> Dict[str, Any]:
        target_field_trim = str(row.get('target_field_trim', '') or '').strip()
        tokens, token_bag = self._tokenize(target_field_trim)

        return {
            'source': 'ETN',
            'raw': row,
            'table_name': row.get('knx_table'),
            'field_name_trim': target_field_trim,
            'normalized_tokens': tokens,
            'normalized_keys': self._generate_keys(tokens),
            'token_bag': token_bag,
            'trl_domain': row.get('trl_domain'),
        }

    def _reconcile_table(
        self,
        table_name: str,
        knx_records: List[Dict[str, Any]],
        etn_records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        matches: List[Dict[str, Any]] = []

        for etn_record in etn_records:
            best_match = self._find_best_match(etn_record, knx_records)

            match_payload = {
                'table_name': table_name,
                'etn': etn_record,
                'knx': best_match,
                'match_type': 'matched' if best_match else 'unmatched'
            }
            matches.append(match_payload)

        for knx_record in knx_records:
            if any(match['knx'] is knx_record for match in matches):
                continue

            matches.append({
                'table_name': table_name,
                'etn': None,
                'knx': knx_record,
                'match_type': 'knx_only'
            })

        return matches

    def _find_best_match(
        self,
        etn_record: Dict[str, Any],
        knx_records: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        best_score = 0
        best_match: Optional[Dict[str, Any]] = None

        for knx_record in knx_records:
            score = self._score_match(etn_record, knx_record)
            if score > best_score:
                best_score = score
                best_match = knx_record

        return best_match

    def _score_match(self, etn_record: Dict[str, Any], knx_record: Dict[str, Any]) -> int:
        score = 0

        etn_tokens = set(etn_record['normalized_tokens'])
        knx_tokens = set(knx_record['normalized_tokens'])

        overlap = etn_tokens.intersection(knx_tokens)
        score += len(overlap) * 5

        if etn_record['normalized_keys'].intersection(knx_record['normalized_keys']):
            score += 10

        etn_bag = etn_record['token_bag']
        knx_bag = knx_record['token_bag']

        bag_overlap = etn_bag.intersection(knx_bag)
        score += len(bag_overlap) * 2

        if etn_record['field_name_trim'].lower() == knx_record['field_name_trim'].lower():
            score += 15

        return score

    def _tokenize(self, value: Any) -> Tuple[List[str], set]:
        if value is None:
            return [], set()
        if not isinstance(value, str):
            value = str(value)
        value = value.strip()
        if not value:
            return [], set()

        parts = re.split(r'[._\s]+', value)
        normalized_segments: List[str] = []
        token_bag: set = set()

        for part in parts:
            if not part:
                continue

            cleaned = re.sub(r'[^A-Za-z0-9]', '', part)
            if not cleaned:
                continue

            lower = cleaned.lower()
            if lower == 'value':
                continue

            if lower.endswith('ies') and len(lower) > 3:
                lower = lower[:-3] + 'y'
            elif lower.endswith('ses') and len(lower) > 3:
                lower = lower[:-2]
            elif lower.endswith('s') and len(lower) > 3:
                lower = lower[:-1]

            normalized_segments.append(lower)
            token_bag.add(lower)
            token_bag.update(self._split_words(lower))

        return normalized_segments, token_bag

    @staticmethod
    def _split_words(segment: str) -> Iterable[str]:
        words = re.findall(r'[a-z]+|\d+', segment)
        return {word for word in words if word}

    @staticmethod
    def _generate_keys(tokens: List[str]) -> set:
        keys = set()
        if not tokens:
            return keys

        for idx in range(len(tokens)):
            tail = tokens[idx:]
            if tail:
                keys.add('.'.join(tail))

        return keys

    # ------------------------------------------------------------------
    # Assembly
    # ------------------------------------------------------------------
    def _assemble_rows(self, matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        assembled_rows: List[Dict[str, Any]] = []

        for payload in matches:
            table_name = payload['table_name']
            etn = payload['etn']
            knx = payload['knx']
            match_type = payload['match_type']

            if etn:
                row = self._assemble_etn_row(table_name, etn, knx, match_type)
            else:
                row = self._assemble_knx_row(table_name, knx)

            assembled_rows.append(row)

        return assembled_rows

    def _assemble_etn_row(
        self,
        table_name: str,
        etn: Dict[str, Any],
        knx: Optional[Dict[str, Any]],
        match_type: str
    ) -> Dict[str, Any]:
        etn_raw = etn['raw']
        knx_raw = knx['raw'] if knx else None

        match_status = self._determine_match_status(etn_raw, knx_raw, match_type)
        domain_name = etn_raw.get('trl_domain')

        maestro_is_key = None
        maestro_data_type = None
        maestro_field_description = None

        if knx_raw is not None:
            maestro_is_key = self._to_bool(knx_raw.get('knx_column_is_key'))
            maestro_data_type = knx_raw.get('knx_column_data_type')
            maestro_field_description = knx_raw.get('knx_column_description')

        assembled = {
            'canonical_entity_name': table_name,
            'maestro_table_name': table_name,
            'maestro_table_description': (knx_raw.get('table_description') if knx_raw is not None else None),
            'erp_technical_table_name': etn_raw.get('source_table'),
            'canonical_attribute_name': etn_raw.get('target_field'),
            'maestro_field_name': etn_raw.get('target_field'),
            'maestro_field_description': maestro_field_description or etn_raw.get('notes'),
            'maestro_data_type': maestro_data_type,
            'maestro_is_key': maestro_is_key,
            'information_only': self._to_bool(etn_raw.get('show_output')),
            'standard_maestro_field': None,
            'add_to_etl': self._to_bool(etn_raw.get('add_to_etl')),
            'default_value': etn_raw.get('constant_value'),
            'example_value': etn_raw.get('example_value'),
            'erp_tcode': None,
            'erp_screen_name': None,
            'erp_screen_field_name': None,
            'erp_technical_table_name_secondary': None,
            'erp_technical_field_name': etn_raw.get('source_field'),
            'etl_logic': etn_raw.get('special_extract_logic'),
            'etl_transformation_table': etn_raw.get('transformation_table_name'),
            'notes': etn_raw.get('notes'),
            'field_output_order': etn_raw.get('sort_output'),
            'match_status': match_status,
            'match_tier': self._derive_match_tier(match_status, knx_raw),
            'match_details': None,
            'sap_augmentation_strategy': None,
            'domain_name': domain_name,
        }

        sap_strategy = self._derive_sap_strategy(
            primary_table=etn_raw.get('source_table'),
            source_field=etn_raw.get('source_field'),
            maestro_field_name=etn_raw.get('target_field'),
            maestro_field_description=maestro_field_description,
            maestro_table_name=table_name,
        )

        assembled['erp_tcode'] = sap_strategy['erp_tcode']
        assembled['erp_screen_name'] = sap_strategy['erp_screen_name']
        assembled['erp_screen_field_name'] = sap_strategy['erp_screen_field_name']
        assembled['sap_augmentation_strategy'] = sap_strategy['strategy']

        return assembled

    def _assemble_knx_row(self, table_name: str, knx: Dict[str, Any]) -> Dict[str, Any]:
        knx_raw = knx['raw']
        field_name = knx_raw.get('field_name_trim') or knx_raw.get('field_name')

        return {
            'canonical_entity_name': table_name,
            'maestro_table_name': table_name,
            'maestro_table_description': knx_raw.get('table_description'),
            'erp_technical_table_name': None,
            'canonical_attribute_name': field_name,
            'maestro_field_name': field_name,
            'maestro_field_description': knx_raw.get('description'),
            'maestro_data_type': knx_raw.get('data_type'),
            'maestro_is_key': self._to_bool(knx_raw.get('is_key')),
            'information_only': None,
            'standard_maestro_field': None,
            'add_to_etl': None,
            'default_value': None,
            'example_value': None,
            'erp_tcode': None,
            'erp_screen_name': None,
            'erp_screen_field_name': None,
            'erp_technical_table_name_secondary': None,
            'erp_technical_field_name': None,
            'etl_logic': None,
            'etl_transformation_table': None,
            'notes': None,
            'field_output_order': None,
            'match_status': 'KNX_ONLY',
            'match_tier': self._derive_match_tier('KNX_ONLY', knx_raw),
            'match_details': None,
            'sap_augmentation_strategy': None,
            'domain_name': None,
        }

    def _derive_match_tier(self, match_status: str, knx_raw: Optional[pd.Series]) -> Optional[int]:
        status = (match_status or '').upper()
        if status == 'MATCHED':
            return 1
        if status == 'ETN_ONLY':
            return 2
        if status == 'KNX_ONLY':
            return 3
        key_flag = str(knx_raw.get('is_key')).lower() if knx_raw is not None else ''
        return 2 if key_flag in {'yes', 'true', 'y', '1'} else 3

    def _determine_match_status(
        self,
        etn_raw: pd.Series,
        knx_raw: Optional[pd.Series],
        match_type: str
    ) -> str:
        if match_type == 'matched':
            return 'MATCHED'
        if match_type == 'knx_only':
            return 'KNX_ONLY'
        return 'ETN_ONLY'

    @staticmethod
    def _to_bool(value: Any) -> Optional[bool]:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        value_str = str(value).strip().lower()
        if value_str in {'true', 'yes', 'y', '1'}:
            return True
        if value_str in {'false', 'no', 'n', '0'}:
            return False
        return None

    # ------------------------------------------------------------------
    # SAP Strategy helpers
    # ------------------------------------------------------------------
    def _derive_sap_strategy(
        self,
        primary_table: Optional[str],
        source_field: Optional[str],
        maestro_field_name: Optional[str],
        maestro_field_description: Optional[str],
        maestro_table_name: Optional[str],
    ) -> Dict[str, Optional[str]]:
        if not primary_table and not source_field:
            return {
                'erp_tcode': None,
                'erp_screen_name': None,
                'erp_screen_field_name': None,
                'strategy': None,
            }

        strategy_parts: List[str] = []
        erp_tcode = None
        erp_screen_name = None

        primary_table, table_source, primary_confident = self._select_sap_table([
            ('maestro_field_name', maestro_field_name),
            ('source_field', source_field),
            ('maestro_field_description', maestro_field_description),
            ('maestro_table_name', maestro_table_name),
            ('primary_table_name', primary_table),
        ])

        if primary_table:
            erp_tcode, erp_screen_name = self.SAP_TABLE_HINTS.get(
                primary_table.lower(),
                (None, None)
            )
            strategy_parts.append(f'{table_source}_table_inferred')
            if primary_confident:
                strategy_parts.append('hint_confident')

        field_token, field_source = self._select_sap_field(
            primary_table,
            source_field,
            maestro_field_name,
            maestro_field_description,
            maestro_table_name,
        )

        if field_token:
            erp_screen_field_name = field_token
            strategy_parts.append(f'{field_source}_field_inferred')
        else:
            erp_screen_field_name = None

        strategy = '+'.join(strategy_parts) if strategy_parts else 'insufficient_source_metadata'

        return {
            'erp_tcode': erp_tcode,
            'erp_screen_name': erp_screen_name,
            'erp_screen_field_name': erp_screen_field_name,
            'strategy': strategy,
        }

    def _select_sap_table(
        self,
        sources: List[Tuple[str, Optional[str]]]
    ) -> Tuple[Optional[str], Optional[str], bool]:
        tokenized = [
            (label, self._tokenize_identifier_parts(value))
            for label, value in sources
        ]

        for label, tokens in tokenized:
            for token_upper, original in tokens:
                if not self._is_valid_identifier(token_upper):
                    continue
                if not (original.isupper() or original.islower()):
                    continue
                if token_upper.lower() in self.SAP_TABLE_HINTS:
                    return token_upper, label, True

        for label, tokens in tokenized:
            for token_upper, original in tokens:
                if not self._is_valid_identifier(token_upper):
                    continue
                if not (original.isupper() or original.islower()):
                    continue
                return token_upper, label, False

        return None, None, False

    def _select_sap_field(
        self,
        primary_table: Optional[str],
        source_field: Optional[str],
        maestro_field_name: Optional[str],
        maestro_field_description: Optional[str],
        maestro_table_name: Optional[str],
    ) -> Tuple[Optional[str], str]:
        tokens = self._tokenize_identifier_parts(source_field)
        if not tokens:
            return None, ''

        primary_table_upper = primary_table.upper() if primary_table else None
        maestro_tokens = self._collect_maestro_tokens(
            maestro_field_name,
            maestro_field_description,
            maestro_table_name,
        )

        best_token: Optional[str] = None
        best_score: Optional[int] = None

        for idx, (token_upper, original) in enumerate(tokens):
            if not self._is_valid_identifier(token_upper):
                continue
            if primary_table_upper and token_upper == primary_table_upper:
                continue
            if not (original.isupper() or original.islower()):
                continue

            score = 0
            if original.isupper():
                score += 5
            elif original.islower():
                score += 3
            elif original[0].isalpha() and original[0].isupper() and original[1:].islower():
                score -= 2

            if re.search(r'\d', original):
                score += 1

            score += max(0, 3 - idx)

            if maestro_tokens and token_upper in maestro_tokens:
                score += 1

            if best_token is None or score > best_score:
                best_token = token_upper
                best_score = score

        return (best_token, 'source_field') if best_token else (None, '')

    def _collect_maestro_tokens(self, *values: Optional[str]) -> set:
        tokens: set = set()
        for value in values:
            for token_upper, original in self._tokenize_identifier_parts(value):
                if self._is_valid_identifier(token_upper):
                    tokens.add(token_upper)
        return tokens

    @staticmethod
    def _tokenize_identifier_parts(value: Optional[Any]) -> List[Tuple[str, str]]:
        if not value:
            return []
        if not isinstance(value, str):
            value = str(value)
        parts = re.split(r'[^A-Za-z0-9]+', value)
        tokens: List[Tuple[str, str]] = []
        for part in parts:
            cleaned = part.strip()
            if not cleaned:
                continue
            tokens.append((cleaned.upper(), cleaned))
        return tokens

    @staticmethod
    def _is_valid_identifier(token: str) -> bool:
        if not token or len(token) < 3:
            return False
        return bool(re.fullmatch(r'[A-Z0-9]+', token))

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _persist_rows(self, con: duckdb.DuckDBPyConnection, rows: List[Dict[str, Any]]) -> None:
        self._ensure_schema(con)

        if not rows:
            self.logger.warning("No ETN CDM mapping rows generated; skipping persistence")
            con.commit()
            return

        self.logger.debug("Deleting existing ETN CDM mapping data")
        con.execute("TRUNCATE TABLE etn_cdm_mappings")

        columns = [
            'canonical_entity_name',
            'maestro_table_name',
            'maestro_table_description',
            'erp_technical_table_name',
            'canonical_attribute_name',
            'maestro_field_name',
            'maestro_field_description',
            'maestro_data_type',
            'maestro_is_key',
            'information_only',
            'standard_maestro_field',
            'add_to_etl',
            'default_value',
            'example_value',
            'erp_tcode',
            'erp_screen_name',
            'erp_screen_field_name',
            'erp_technical_table_name_secondary',
            'erp_technical_field_name',
            'etl_logic',
            'etl_transformation_table',
            'notes',
            'field_output_order',
            'match_status',
            'match_tier',
            'match_details',
            'sap_augmentation_strategy',
            'domain_name',
        ]

        placeholders = ', '.join(['?'] * len(columns))
        insert_sql = f"INSERT INTO etn_cdm_mappings ({', '.join(columns)}) VALUES ({placeholders})"

        for row in rows:
            values = [row.get(col) for col in columns]
            con.execute(insert_sql, values)

        con.commit()

    def _ensure_schema(self, con: duckdb.DuckDBPyConnection) -> None:
        con.execute("""
            CREATE TABLE IF NOT EXISTS etn_cdm_mappings (
                canonical_entity_name VARCHAR,
                maestro_table_name VARCHAR,
                maestro_table_description VARCHAR,
                erp_technical_table_name VARCHAR,
                canonical_attribute_name VARCHAR,
                maestro_field_name VARCHAR,
                maestro_field_description VARCHAR,
                maestro_data_type VARCHAR,
                maestro_is_key BOOLEAN,
                information_only BOOLEAN,
                standard_maestro_field BOOLEAN,
                add_to_etl BOOLEAN,
                default_value VARCHAR,
                example_value VARCHAR,
                erp_tcode VARCHAR,
                erp_screen_name VARCHAR,
                erp_screen_field_name VARCHAR,
                erp_technical_table_name_secondary VARCHAR,
                erp_technical_field_name VARCHAR,
                etl_logic VARCHAR,
                etl_transformation_table VARCHAR,
                notes VARCHAR,
                field_output_order INTEGER,
                match_status VARCHAR,
                match_tier INTEGER,
                match_details TEXT,
                sap_augmentation_strategy VARCHAR,
                domain_name VARCHAR
            )
        """)


class EtnCdmUpserter:
    """Populate the summarized etn_cdm table from Trillium augmentation and KNX metadata."""

    def __init__(self, db_path: str = "mappings.duckdb", logger: Optional[logging.Logger] = None):
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__)

    def run(self, con: Optional[duckdb.DuckDBPyConnection] = None) -> None:
        manage_connection = False

        if con is None:
            con = duckdb.connect(self.db_path)
            manage_connection = True

        try:
            self.logger.info("Starting ETN CDM aggregation upsert")
            cdm_df = self._load_trl_cdm_augmentation(con)
            if cdm_df.empty:
                self.logger.warning("trl_cdm_augmentation table is empty; clearing etn_cdm")
                self._persist_rows(con, [])
                return

            keys_lookup = self._build_keys_lookup(con)
            relationships_lookup = self._build_relationships_lookup(con)
            rows = self._assemble_rows(cdm_df, keys_lookup, relationships_lookup)
            self._persist_rows(con, rows)
            self.logger.info("Completed ETN CDM aggregation upsert with %d rows", len(rows))
        finally:
            if manage_connection:
                con.close()

    def _load_trl_cdm_augmentation(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        return con.execute(
            """
            SELECT
                domain,
                domain_description,
                entity,
                entity_description,
                applications
            FROM trl_cdm_augmentation
        """
        ).fetchdf()

    def _build_keys_lookup(self, con: duckdb.DuckDBPyConnection) -> Dict[str, str]:
        keys_df = con.execute(
            """
            SELECT
                upper(trim(t.name)) AS table_name_upper,
                string_agg(c.field_name, ', ') AS keys
            FROM knx_doc_columns AS c
            JOIN knx_doc_tables AS t ON c.table_id = t.id
            WHERE lower(trim(coalesce(c.is_key, ''))) IN ('yes', 'y', 'true', '1')
            GROUP BY table_name_upper
        """
        ).fetchdf()
        return {
            row['table_name_upper']: row['keys']
            for _, row in keys_df.iterrows()
            if row['table_name_upper']
        }

    def _build_relationships_lookup(self, con: duckdb.DuckDBPyConnection) -> Dict[str, str]:
        relationships_df = con.execute(
            """
            SELECT
                upper(trim(t.name)) AS table_name_upper,
                c.field_name,
                coalesce(rt.name, '') AS referenced_table_name,
                c.description
            FROM knx_doc_columns AS c
            JOIN knx_doc_tables AS t ON c.table_id = t.id
            LEFT JOIN knx_doc_tables AS rt ON c.referenced_table_id = rt.id
            WHERE lower(trim(coalesce(c.data_type, ''))) LIKE 'reference%'
        """
        ).fetchdf()

        relationships: Dict[str, List[str]] = defaultdict(list)
        table_name_pattern = re.compile(r'^[A-Za-z0-9_]+')

        def normalize_table_name(raw_value: object) -> str:
            if not isinstance(raw_value, str):
                return ""
            text = raw_value.strip()
            if not text:
                return ""
            match = table_name_pattern.match(text)
            if match:
                return match.group(0)
            parts = text.split()
            return parts[0] if parts else ""

        for _, row in relationships_df.iterrows():
            table_name_upper = row.get('table_name_upper')
            if not table_name_upper:
                continue

            field_name = str(row.get('field_name', '') or '').strip()
            if not field_name:
                continue

            referenced_table = normalize_table_name(row.get('referenced_table_name', ''))
            if not referenced_table:
                description = row.get('description')
                if isinstance(description, str):
                    match = re.search(r'Referenced table:\s*([^\n\r;]+)', description, flags=re.IGNORECASE)
                    if match:
                        referenced_table = normalize_table_name(match.group(1))

            if not referenced_table:
                continue

            relationships[table_name_upper].append(f"{field_name} -> {referenced_table}")

        return {
            table: ', '.join(entries)
            for table, entries in relationships.items()
            if entries
        }

    def _assemble_rows(
        self,
        cdm_df: pd.DataFrame,
        keys_lookup: Dict[str, str],
        relationships_lookup: Dict[str, str],
    ) -> List[Dict[str, Optional[str]]]:
        rows: List[Dict[str, Optional[str]]] = []
        for _, record in cdm_df.iterrows():
            entity = record.get('entity')
            entity_upper = entity.strip().upper() if isinstance(entity, str) else None
            rows.append(
                {
                    'domain': record.get('domain'),
                    'domain_description': record.get('domain_description'),
                    'entity': entity,
                    'entity_description': record.get('entity_description'),
                    'keys': keys_lookup.get(entity_upper, '') if entity_upper else '',
                    'relationships': relationships_lookup.get(entity_upper, '') if entity_upper else '',
                    'applications': record.get('applications'),
                }
            )
        return rows

    def _persist_rows(self, con: duckdb.DuckDBPyConnection, rows: List[Dict[str, Optional[str]]]) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS etn_cdm (
                domain VARCHAR,
                domain_description VARCHAR,
                entity VARCHAR,
                entity_description VARCHAR,
                keys VARCHAR,
                relationships VARCHAR,
                applications VARCHAR
            )
        """
        )
        con.execute("TRUNCATE TABLE etn_cdm")

        if not rows:
            con.commit()
            return

        columns = [
            'domain',
            'domain_description',
            'entity',
            'entity_description',
            'keys',
            'relationships',
            'applications',
        ]
        placeholders = ', '.join(['?'] * len(columns))
        insert_sql = f"INSERT INTO etn_cdm ({', '.join(columns)}) VALUES ({placeholders})"

        for row in rows:
            con.execute(insert_sql, [row.get(col) for col in columns])

        con.commit()
