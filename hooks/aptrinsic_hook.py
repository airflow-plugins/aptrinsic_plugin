from airflow.hooks.http_hook import HttpHook


class AptrinsicHook(HttpHook):

    def __init__(self, aptrinsic_conn_id):
        super().__init__(http_conn_id=aptrinsic_conn_id,
                         method='GET')

    def run(self, endpoint, data=None, headers=None):
        conn = self.get_connection(self.http_conn_id)
        headers = {"X-APTRINSIC-API-KEY": "{0}".format(conn.password)}
        return super().run(endpoint, data, headers)
