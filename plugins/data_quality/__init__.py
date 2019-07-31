from airflow.plugins_manager import AirflowPlugin
from data_quality.operators.data_quality import DataQualityOperator

# Defining the plugin class
class DataQualityPlugins(AirflowPlugin):
    name = "data_quality_plugin"
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    operators = [DataQualityOperator]
