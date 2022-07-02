from .other import append_to_file, dump_json

from datetime import datetime
import AddFilesystem


class Logger:
    """Class to log events, config, errors, successes"""

    def __init__(self, filesystem: AddFilesystem, app_name: str, log_path: str):

        self.filesystem = filesystem
        self.app_name = app_name + str(datetime.now().strftime("_%Y%m%d_%H%M"))
        self.log_path = log_path
        self.app_path = self.log_path + "master_log/" + self.app_name + "/"

    def log_config(self, config: dict) -> None:
        """Dumps config in a text file"""
        app_config_path = self.app_path + "config"

        dump_json(self.filesystem, app_config_path, config)

    def log_event(self, text: str) -> None:
        """Writes events, i.e. each time next sg starts processing"""
        app_event_path = self.app_path + "events"

        dt = datetime.now()
        event_text = f"{dt}\t {text}"
        append_to_file(self.filesystem, app_event_path, [event_text])

    def log_error(self, text: str) -> None:
        """Logs erros"""
        app_errros_path = self.app_path + "errors"

        dt = datetime.now()
        error_text = f"{dt}\t {text}"
        append_to_file(self.filesystem, app_errros_path, [error_text])

    def log_warning(self, text: str) -> None:
        """Logs warnings"""
        app_warnings_path = self.app_path + "warnings"

        dt = datetime.now()
        warning_text = f"{dt}\t {text}"
        append_to_file(self.filesystem, app_warnings_path, [warning_text])

    def log_success(self, path: str, rows: list) -> None:
        """Writes successfully processed parquets (URLs)"""
        append_to_file(self.filesystem, path, rows)
