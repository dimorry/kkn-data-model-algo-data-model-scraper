import logging
import sys


class LoggerConfig:
    """Centralized logging configuration and management"""
    
    def __init__(self, name=None, log_level=logging.INFO, log_file=None, 
                 console_format=None, file_format=None):
        self.name = name or __name__
        self.log_level = log_level
        self.log_file = log_file
        self.console_format = console_format or "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        self.file_format = file_format or "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
        self.logger = None
        
    def setup_logger(self):
        """Configure and return a logger instance"""
        # Configure root logger
        logging.basicConfig(
            level=self.log_level,
            format=self.console_format,
            handlers=[]
        )
        
        self.logger = logging.getLogger(self.name)
        self.logger.handlers.clear()
        self.logger.setLevel(self.log_level)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)
        console_formatter = logging.Formatter(self.console_format)
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler (optional)
        if self.log_file:
            file_handler = logging.FileHandler(self.log_file)
            file_handler.setLevel(self.log_level)
            file_formatter = logging.Formatter(self.file_format)
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
        
        return self.logger
    
    def get_logger(self):
        """Get the configured logger instance"""
        if not self.logger:
            return self.setup_logger()
        return self.logger
    
    def set_level(self, level):
        """Change the logging level"""
        self.log_level = level
        if self.logger:
            self.logger.setLevel(level)
            for handler in self.logger.handlers:
                handler.setLevel(level)
    
    def add_file_handler(self, filepath):
        """Add a file handler to existing logger"""
        if not self.logger:
            self.setup_logger()
            
        file_handler = logging.FileHandler(filepath)
        file_handler.setLevel(self.log_level)
        file_formatter = logging.Formatter(self.file_format)
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
    def remove_handlers(self):
        """Remove all handlers from logger"""
        if self.logger:
            for handler in self.logger.handlers[:]:
                self.logger.removeHandler(handler)