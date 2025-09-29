import logging
import re
from typing import Dict, List, Optional, Tuple, Any, Iterable

import duckdb
import pandas as pd


class EtnCdmUpserter:
    """Generate and load ETN CDM records by reconciling KNX metadata with ETN mappings."""

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
            self.logger.info("Starting ETN CDM upsert process")

            data = self._load_source_data(con)
            matched_rows = self._match_records(data)
            assembled_rows = self._assemble_rows(matched_rows)

            self._persist_rows(con, assembled_rows)
            self.logger.info(f"Completed ETN CDM upsert with {len(assembled_rows)} rows")

        except Exception as exc:
            self.logger.error(f"Failed ETN CDM upsert: {exc}")
            raise
        finally:
            if manage_connection:
                con.close()

    # ------------------------------------------------------------------
    # Data Loading
    # ------------------------------------------------------------------
    def _load_source_data(self, con: duckdb.DuckDBPyConnection) -> Dict[str, pd.DataFrame]:
        self.logger.debug("Loading KNX and ETN source data")

        knx_expanded = con.execute(
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
                display_on_export,
                created_at
            FROM knx_doc_expanded
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
                id,
                knx_table,
                target_field,
                source_table,
                source_field,
                special_extract_logic,
                transformation_table_name,
                constant_value,
                example_value,
                notes,
                show_output,
                sort_output
            FROM etn_doc_mappings
        """
        ).fetchdf()

        # Preprocess KNX expanded data
        knx_expanded['field_name_trim'] = knx_expanded['field_name'].astype(str).str.strip()
        knx_expanded['data_type_lower'] = knx_expanded['data_type'].astype(str).str.lower()
        knx_expanded = knx_expanded[~knx_expanded['data_type_lower'].str.startswith('reference', na=False)].copy()

        knx_expanded = knx_expanded.merge(
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

        knx_expanded = knx_expanded.merge(
            columns_df[['table_id', 'field_name_trim', 'knx_column_id', 'knx_column_field_name',
                        'knx_column_description', 'knx_column_data_type', 'knx_column_is_key']],
            left_on=['table_id', 'field_name_trim'],
            right_on=['table_id', 'field_name_trim'],
            how='left'
        )

        etn_mappings['target_field_trim'] = etn_mappings['target_field'].astype(str).str.strip()

        return {
            'knx_expanded': knx_expanded,
            'etn_mappings': etn_mappings,
        }

    # ------------------------------------------------------------------
    # Matching Logic
    # ------------------------------------------------------------------
    def _match_records(self, data: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        knx_df = data['knx_expanded']
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
            'target_field_trim': target_field_trim,
            'normalized_tokens': tokens,
            'normalized_keys': self._generate_keys(tokens),
            'token_bag': token_bag,
        }

    def _reconcile_table(
        self,
        table_name: str,
        knx_records: List[Dict[str, Any]],
        etn_records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        matches: List[Dict[str, Any]] = []

        etn_available = set(range(len(etn_records)))
        knx_remaining: List[int] = []

        # Tier 1: direct match on trimmed names (case-insensitive)
        direct_lookup: Dict[str, List[int]] = {}
        for idx, etn in enumerate(etn_records):
            key = etn['target_field_trim'].lower()
            direct_lookup.setdefault(key, []).append(idx)

        for k_idx, knx in enumerate(knx_records):
            key = knx['field_name_trim'].lower()
            candidate_indexes = direct_lookup.get(key)
            matched = False

            if candidate_indexes:
                for etn_index in candidate_indexes:
                    if etn_index in etn_available:
                        etn_available.remove(etn_index)
                        matches.append(self._build_match_payload(
                            table_name, knx, etn_records[etn_index], 1,
                            f"Direct match on field '{knx['field_name_trim']}'"
                        ))
                        matched = True
                        break

            if not matched:
                knx_remaining.append(k_idx)

        # Tier 2: normalized dotted key intersection (ordered)
        still_remaining: List[int] = []
        for k_idx in knx_remaining:
            knx = knx_records[k_idx]
            best_score = 0
            best_etn_index = None
            best_key = None

            for etn_index in list(etn_available):
                etn = etn_records[etn_index]
                overlap = knx['normalized_keys'] & etn['normalized_keys']
                if not overlap:
                    continue

                # Choose the longest matching key (by segments)
                candidate_key = max(overlap, key=lambda k: (k.count('.') + 1, len(k)))
                score = candidate_key.count('.') + 1

                if score > best_score:
                    best_score = score
                    best_etn_index = etn_index
                    best_key = candidate_key

            if best_etn_index is not None:
                etn_available.remove(best_etn_index)
                matches.append(self._build_match_payload(
                    table_name,
                    knx,
                    etn_records[best_etn_index],
                    2,
                    f"Normalized key match on '{best_key}'"
                ))
            else:
                still_remaining.append(k_idx)

        # Tier 3: token bag overlap (order-insensitive)
        final_remaining: List[int] = []
        for k_idx in still_remaining:
            knx = knx_records[k_idx]
            best_score = 0
            best_etn_index = None
            token_bag_knx = knx['token_bag']

            for etn_index in list(etn_available):
                etn = etn_records[etn_index]
                overlap = token_bag_knx & etn['token_bag']
                score = len(overlap)

                if score > best_score and score >= 2:
                    best_score = score
                    best_etn_index = etn_index

            if best_etn_index is not None:
                etn_available.remove(best_etn_index)
                etn = etn_records[best_etn_index]
                matches.append(self._build_match_payload(
                    table_name,
                    knx,
                    etn,
                    3,
                    f"Token overlap match with tokens {sorted(knx['token_bag'] & etn['token_bag'])}"
                ))
            else:
                final_remaining.append(k_idx)

        # Remaining KNX records (no match)
        for k_idx in final_remaining:
            matches.append(self._build_match_payload(
                table_name,
                knx_records[k_idx],
                None,
                0,
                "No ETN match found"
            ))

        # Remaining ETN records (no match)
        for etn_index in etn_available:
            matches.append(self._build_match_payload(
                table_name,
                None,
                etn_records[etn_index],
                0,
                "No KNX match found"
            ))

        return matches

    def _build_match_payload(
        self,
        table_name: str,
        knx: Optional[Dict[str, Any]],
        etn: Optional[Dict[str, Any]],
        tier: int,
        details: str
    ) -> Dict[str, Any]:
        status = 'MATCHED' if knx and etn else ('KNX_ONLY' if knx else 'ETN_ONLY')
        return {
            'table_name': table_name,
            'knx': knx,
            'etn': etn,
            'match_tier': tier if status == 'MATCHED' else 0,
            'match_status': status,
            'match_details': details,
        }

    # ------------------------------------------------------------------
    # Row Assembly
    # ------------------------------------------------------------------
    def _assemble_rows(self, matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        assembled: List[Dict[str, Any]] = []

        for payload in matches:
            knx = payload['knx']
            etn = payload['etn']

            table_name = payload['table_name']
            table_description = None
            canonical_attribute_name = None
            maestro_field_name = None
            maestro_field_description = None
            maestro_data_type = None
            maestro_is_key = False
            information_only = False
            standard_maestro_field = False
            etn_target_field = None

            if knx:
                canon_field = knx['field_name_trim'] or None
                canonical_attribute_name = canon_field
                table_description = knx.get('table_description')
                maestro_field_name = knx.get('knx_column_field_name')
                maestro_field_description = knx.get('knx_column_description')
                maestro_data_type = knx.get('knx_column_data_type')
                maestro_is_key = self._to_bool(knx.get('knx_column_is_key'))
                standard_maestro_field = maestro_field_name is not None and pd.notna(maestro_field_name)

            if etn:
                information_only = not standard_maestro_field
                etn_target_field = etn.get('target_field_trim') or None
            else:
                information_only = False

            final_full_field_name = None
            for candidate in (canonical_attribute_name, maestro_field_name, etn_target_field):
                normalized = self._safe_str(candidate)
                if normalized:
                    final_full_field_name = normalized
                    break

            etn_raw = etn['raw'] if etn else None
            knx_raw = knx['raw'] if knx else None
            knx_description = None
            knx_data_type = None
            if knx_raw is not None:
                if 'description' in knx_raw:
                    knx_description = self._safe_str(knx_raw['description'])
                if 'data_type' in knx_raw:
                    knx_data_type = self._safe_str(knx_raw['data_type'])

            canonical_attribute_name = (
                self._collapse_field_path(final_full_field_name)
                if final_full_field_name
                else None
            )
            maestro_field_name = final_full_field_name
            canonical_attribute_name = self._safe_str(canonical_attribute_name)
            maestro_field_name = self._safe_str(maestro_field_name)
            maestro_field_description = self._safe_str(maestro_field_description)
            maestro_data_type = self._safe_str(maestro_data_type)

            if not maestro_field_description and knx_description:
                maestro_field_description = knx_description

            if maestro_field_name and not maestro_field_description:
                maestro_field_description = canonical_attribute_name or maestro_field_name

            if maestro_field_name and not maestro_data_type:
                maestro_data_type = knx_data_type

            source_table = self._safe_str(etn_raw['source_table']) if etn_raw is not None and 'source_table' in etn_raw else None
            source_field = self._safe_str(etn_raw['source_field']) if etn_raw is not None and 'source_field' in etn_raw else None
            notes = self._safe_str(etn_raw['notes']) if etn_raw is not None and 'notes' in etn_raw else None

            sap_aug = self._augment_sap_info(
                source_table=source_table,
                source_field=source_field,
                notes=notes,
                maestro_table_name=table_name,
                maestro_field_name=maestro_field_name,
                maestro_field_description=maestro_field_description,
            )

            row = {
                'canonical_entity_name': table_name,
                'maestro_table_name': table_name,
                'maestro_table_description': table_description,
                'erp_technical_table_name': source_table,
                'canonical_attribute_name': canonical_attribute_name,
                'maestro_field_name': maestro_field_name,
                'maestro_field_description': maestro_field_description,
                'maestro_data_type': maestro_data_type,
                'maestro_is_key': maestro_is_key,
                'information_only': information_only,
                'standard_maestro_field': standard_maestro_field,
                'add_to_etl': self._should_add_to_etl(etn_raw),
                'default_value': self._safe_str(etn_raw['constant_value']) if etn_raw is not None and 'constant_value' in etn_raw else None,
                'example_value': self._safe_str(etn_raw['example_value']) if etn_raw is not None and 'example_value' in etn_raw else None,
                'erp_tcode': sap_aug['erp_tcode'],
                'erp_screen_name': sap_aug['erp_screen_name'],
                'erp_screen_field_name': sap_aug['erp_screen_field_name'],
                'erp_technical_table_name_secondary': None,
                'erp_technical_field_name': source_field,
                'etl_logic': self._safe_str(etn_raw['special_extract_logic']) if etn_raw is not None and 'special_extract_logic' in etn_raw else None,
                'etl_transformation_table': self._safe_str(etn_raw['transformation_table_name']) if etn_raw is not None and 'transformation_table_name' in etn_raw else None,
                'notes': notes,
                'field_output_order': self._safe_int(etn_raw['sort_output']) if etn_raw is not None and 'sort_output' in etn_raw else None,
                'match_status': payload['match_status'],
                'match_tier': payload['match_tier'],
                'match_details': payload['match_details'],
                'sap_augmentation_strategy': sap_aug['strategy'],
            }

            # When there is no ETN record, ensure ETL-related fields are defaulted
            if etn is None:
                row.update({
                    'erp_technical_table_name': None,
                    'erp_technical_field_name': None,
                    'add_to_etl': False,
                    'default_value': None,
                    'example_value': None,
                    'etl_logic': None,
                    'etl_transformation_table': None,
                    'notes': None,
                    'field_output_order': None,
                })

            assembled.append(row)

        return assembled

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _persist_rows(self, con: duckdb.DuckDBPyConnection, rows: List[Dict[str, Any]]) -> None:
        self._ensure_schema(con)

        if not rows:
            self.logger.warning("No ETN CDM rows generated; skipping persistence")
            con.execute("DELETE FROM etn_cdm")
            con.commit()
            return

        self.logger.debug("Deleting existing ETN CDM data")
        con.execute("DELETE FROM etn_cdm")

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
        ]

        placeholders = ', '.join(['?'] * len(columns))
        insert_sql = f"INSERT INTO etn_cdm ({', '.join(columns)}) VALUES ({placeholders})"

        for row in rows:
            values = [row.get(col) for col in columns]
            con.execute(insert_sql, values)

        con.commit()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _ensure_schema(self, con: duckdb.DuckDBPyConnection) -> None:
        """Ensure etn_cdm table exists with provenance columns."""
        con.execute("""
            CREATE TABLE IF NOT EXISTS etn_cdm (
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
                sap_augmentation_strategy VARCHAR
            )
        """)

        con.execute("ALTER TABLE etn_cdm ADD COLUMN IF NOT EXISTS match_status VARCHAR")
        con.execute("ALTER TABLE etn_cdm ADD COLUMN IF NOT EXISTS match_tier INTEGER")
        con.execute("ALTER TABLE etn_cdm ADD COLUMN IF NOT EXISTS match_details TEXT")
        con.execute("ALTER TABLE etn_cdm ADD COLUMN IF NOT EXISTS sap_augmentation_strategy VARCHAR")

    def _tokenize(self, value: str) -> Tuple[List[str], set]:
        if not value:
            return [], set()

        parts = re.split(r'[._\s]+', value.strip())
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

            # plural handling
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
        # Split camel case and compound segments into individual words
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

    @staticmethod
    def _safe_str(value: Any) -> Optional[str]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        text = str(value).strip()
        return text if text else None

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _to_bool(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        return text in {'y', 'yes', 'true', '1'}

    @staticmethod
    def _collapse_field_path(value: str) -> Optional[str]:
        text = value.strip()
        if not text:
            return None

        if '.' not in text:
            return text

        parts = [segment.strip() for segment in text.split('.') if segment.strip()]
        if not parts:
            return None

        tail = parts[-2:] if len(parts) >= 2 else parts[-1:]
        collapsed = ''.join(tail)
        return collapsed or text

    def _should_add_to_etl(self, etn_row: Optional[pd.Series]) -> bool:
        if etn_row is None or 'show_output' not in etn_row:
            return False
        return self._to_bool(etn_row['show_output'])

    def _augment_sap_info(
        self,
        source_table: Optional[str],
        source_field: Optional[str],
        notes: Optional[str],
        maestro_table_name: Optional[str],
        maestro_field_name: Optional[str],
        maestro_field_description: Optional[str]
    ) -> Dict[str, Optional[str]]:
        erp_tcode: Optional[str] = None
        erp_screen_name: Optional[str] = None
        erp_screen_field_name: Optional[str] = None

        primary_table, table_source, has_hint = self._select_sap_table([
            ('source_table', source_table),
            ('source_field', source_field),
            ('notes', notes),
            ('maestro_field_description', maestro_field_description),
        ])

        strategy_parts: List[str] = []

        if primary_table and has_hint:
            hint = self.SAP_TABLE_HINTS[primary_table.lower()]
            erp_tcode, erp_screen_name = hint
            strategy_parts.append(f'{table_source}_table_hint')
        elif primary_table:
            strategy_parts.append(f'{table_source}_table_detected_no_hint')

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
    def _tokenize_identifier_parts(value: Optional[str]) -> List[Tuple[str, str]]:
        if not value:
            return []
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
