from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    check_sql = """
    SELECT
    COUNT(*)
    FROM
    {table}
    WHERE
    {where}
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 queries=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.queries = queries

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info("Redshift hook defined")

        # Running the data quality queries
        for query in self.queries:
            if query.get("table") is None:
                self.log.error("Table name wasn't supplied.")
            else:
                table = query.get("table")

            if query.get("where") is None:
                where = "1=1"
            else:
                where = query.get("where")

            if query.get("result") is None:
                expected_result = 0
            else:
                expected_result = query.get("result")

            # Formatting the SQL
            sql = DataQualityOperator.check_sql.format(table=table, where=where)
            self.log.info(sql)

            # Querying data to Redshift
            records = redshift.get_first(sql)
            if records == 0 or records[0] != expected_result:
                self.log.error(f"""Table [{table}] with filters [{where}] failed passing the data quality test.
                                   Expected Result: [{expected_result}]
                                   Result: [{records[0]}]""")
            else:
                self.log.info(f"Table [{table}] with filters [{where}] has passed data quality test.")
