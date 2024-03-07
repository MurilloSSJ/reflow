from models.dag import DagModel
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import select


class DagModelEntity:
    def get_dagmodel(self, dag_id: str) -> DagModel | None:
        return self.session.get(
            DagModel,
            dag_id,
            options=[joinedload(DagModel.parent_dag)],
        )

    def get_current(self, dag_id: str) -> DagModel:
        return self.session.scalar(
            select(DagModel).where(DagModel.dag_id == dag_id)
        ).first()

    def get_last_dagrun(self, dag: DagModel, include_externally_triggered=False):
        return get_last_dagrun(
            dag.dag_id,
            session=self.session,
            include_externally_triggered=include_externally_triggered,
        )

    def get_paused_dag_ids(self, dag_ids: list[str]) -> set[str]:
        """
        Given a list of dag_ids, get a set of Paused Dag Ids.

        :param dag_ids: List of Dag ids
        :param session: ORM Session
        :return: Paused Dag_ids
        """
        paused_dag_ids = self.session.execute(
            select(DagModel.dag_id)
            .where(DagModel.is_paused == True)
            .where(DagModel.dag_id.in_(dag_ids))
        )

        paused_dag_ids = {paused_dag_id for (paused_dag_id,) in paused_dag_ids}
        return paused_dag_ids

    def set_is_paused(
        self,
        dag: DagModel,
        is_paused: bool,
        including_subdags: bool = True,
    ) -> None:
        """
        Pause/Un-pause a DAG.

        :param is_paused: Is the DAG paused
        :param including_subdags: whether to include the DAG's subdags
        :param session: session
        """
        filter_query = [
            DagModel.dag_id == dag.dag_id,
        ]
        if including_subdags:
            filter_query.append(DagModel.root_dag_id == dag.dag_id)
        self.session.execute(
            update(DagModel)
            .where(or_(*filter_query))
            .values(is_paused=is_paused)
            .execution_options(synchronize_session="fetch")
        )
        self.session.commit()
