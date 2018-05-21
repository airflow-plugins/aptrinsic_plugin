from airflow.plugins_manager import AirflowPlugin
from aptrinsic_plugin.hooks.aptrinsic_hook import AptrinsicHook
from aptrinsic_plugin.operators.aptrinsic_to_s3_operator import AptrinsicToS3Operator


class aptrinsic_plugin(AirflowPlugin):
    name = "aptrinsic_plugin"
    operators = [AptrinsicToS3Operator]
    # Leave in for explicitness
    hooks = [AptrinsicHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
