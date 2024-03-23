from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.settings import get_settings
import time
from src.parser.controller import ParserController

settings = get_settings()
IGNORED_WATCHER_FILES = ["__pycache__"]


class DagParser(FileSystemEventHandler):
    def should_ignore(self, event):
        print("enter in should_ignore")
        for path in IGNORED_WATCHER_FILES:
            if path in event.src_path:
                return True
        return False

    def on_created(self, event):
        if not event.is_directory:
            ParserController.create_dag_file(event.src_path)

    def on_deleted(self, event):
        if event.is_directory:
            ParserController.delete_dag_file()
        else:
            ParserController.delete_dag_file()

    def on_modified(self, event):
        if not event.is_directory and not self.should_ignore(event):
            ParserController.modify_dag_file(event.src_path)


if __name__ == "__main__":
    event_handler = DagParser()
    observer = Observer()
    observer.schedule(event_handler, path=settings.dag_path, recursive=True)
    observer.start()
    while True:
        time.sleep(30)
