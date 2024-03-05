from src.modules.dag.model import DagModel
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

    def deactivate_deleted_dags(
        self,
        alive_dag_filelocs: Container[str],
        processor_subdir: str,
    ) -> None:
        """
        Set ``is_active=False`` on the DAGs for which the DAG files have been removed.

        :param alive_dag_filelocs: file paths of alive DAGs
        :param processor_subdir: dag processor subdir
        :param session: ORM Session
        """
        dag_models = self.session.scalars(
            select(DagModel).where(
                DagModel.fileloc.is_not(None),
                or_(
                    DagModel.processor_subdir.is_(None),
                    DagModel.processor_subdir == processor_subdir,
                ),
            )
        )

        for dag_model in dag_models:
            if dag_model.fileloc not in alive_dag_filelocs:
                dag_model.is_active = False

    def dags_needing_dagruns(
        self,
    ) -> tuple[Query, dict[str, tuple[datetime, datetime]]]:
        """
        Return (and lock) a list of Dag objects that are due to create a new DagRun.

        This will return a resultset of rows that is row-level-locked with a "SELECT ... FOR UPDATE" query,
        you should ensure that any scheduling decisions are made in a single transaction -- as soon as the
        transaction is committed it will be unlocked.
        """

        def dag_ready(
            dag_id: str, cond: BaseDatasetEventInput, statuses: dict
        ) -> bool | None:
            return cond.evaluate(statuses)

        # this loads all the DDRQ records.... may need to limit num dags
        all_records = self.session.scalars(select(DatasetDagRunQueue)).all()
        by_dag = defaultdict(list)
        for r in all_records:
            by_dag[r.target_dag_id].append(r)
        del all_records
        dag_statuses = {}
        for dag_id, records in by_dag.items():
            dag_statuses[dag_id] = {x.dataset.uri: True for x in records}
        ser_dags = self.session.scalars(
            select(SerializedDagModel).where(
                SerializedDagModel.dag_id.in_(dag_statuses.keys())
            )
        ).all()
        for ser_dag in ser_dags:
            dag_id = ser_dag.dag_id
            statuses = dag_statuses[dag_id]
            if not dag_ready(
                dag_id, cond=ser_dag.dag.dataset_triggers, statuses=statuses
            ):
                del by_dag[dag_id]
                del dag_statuses[dag_id]
        del dag_statuses
        dataset_triggered_dag_info = {}
        for dag_id, records in by_dag.items():
            times = sorted(x.created_at for x in records)
            dataset_triggered_dag_info[dag_id] = (times[0], times[-1])
        del by_dag
        dataset_triggered_dag_ids = set(dataset_triggered_dag_info.keys())
        if dataset_triggered_dag_ids:
            exclusion_list = set(
                self.session.scalars(
                    select(DagModel.dag_id)
                    .join(DagRun.dag_model)
                    .where(DagRun.state.in_((DagRunState.QUEUED, DagRunState.RUNNING)))
                    .where(DagModel.dag_id.in_(dataset_triggered_dag_ids))
                    .group_by(DagModel.dag_id)
                    .having(func.count() >= func.max(DagModel.max_active_runs))
                )
            )
            if exclusion_list:
                dataset_triggered_dag_ids -= exclusion_list
                dataset_triggered_dag_info = {
                    k: v
                    for k, v in dataset_triggered_dag_info.items()
                    if k not in exclusion_list
                }

        # We limit so that _one_ scheduler doesn't try to do all the creation of dag runs
        query = (
            select(DagModel)
            .where(
                DagModel.is_paused == False,
                DagModel.is_active == True,
                DagModel.has_import_errors == False,
                or_(
                    DagModel.next_dagrun_create_after <= func.now(),
                    DagModel.dag_id.in_(dataset_triggered_dag_ids),
                ),
            )
            .order_by(DagModel.next_dagrun_create_after)
            .limit(DagModel.NUM_DAGS_PER_DAGRUN_QUERY)
        )

        return (
            self.session.scalars(
                with_row_locks(query, of=cls, session=session, skip_locked=True)
            ),
            dataset_triggered_dag_info,
        )

    def get_dataset_triggered_next_run_info(
        self, dag_ids: list[str]
    ) -> dict[str, int | str] | None:
        """
        Get next run info for a list of dag_ids.

        Given a list of dag_ids, get string representing how close any that are dataset triggered are
        their next run, e.g. "1 of 2 datasets updated".
        """
        return {
            x.dag_id: {
                "uri": x.uri,
                "ready": x.ready,
                "total": x.total,
            }
            for x in self.session.execute(
                select(
                    DagScheduleDatasetReference.dag_id,
                    # This is a dirty hack to workaround group by requiring an aggregate,
                    # since grouping by dataset is not what we want to do here...but it works
                    case(
                        (func.count() == 1, func.max(DatasetModel.uri)), else_=""
                    ).label("uri"),
                    func.count().label("total"),
                    func.sum(case((DDRQ.target_dag_id.is_not(None), 1), else_=0)).label(
                        "ready"
                    ),
                )
                .join(
                    DDRQ,
                    and_(
                        DDRQ.dataset_id == DagScheduleDatasetReference.dataset_id,
                        DDRQ.target_dag_id == DagScheduleDatasetReference.dag_id,
                    ),
                    isouter=True,
                )
                .join(
                    DatasetModel,
                    DatasetModel.id == DagScheduleDatasetReference.dataset_id,
                )
                .group_by(DagScheduleDatasetReference.dag_id)
                .where(DagScheduleDatasetReference.dag_id.in_(dag_ids))
            ).all()
        }
