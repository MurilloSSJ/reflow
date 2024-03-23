from src.parser.decoder import DagFileDecoder


class ParserController:
    @staticmethod
    def create_dag_file(path: str):
        pass

    @staticmethod
    def delete_dag_file(path: str):
        pass

    @staticmethod
    def modify_dag_file(path: str):
        DagFileDecoder(path).decode()
