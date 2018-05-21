import logging
import json
import boa

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator

from aptrinsic_plugin.hooks.aptrinsic_hook import AptrinsicHook


class AptrinsicToS3Operator(BaseOperator):
    """
    Aptrinsic To S3 Operator

    :param aptrinsic_conn_id:   The Aptrinsic connection id.
    :type aptrinsic_conn_id:    string
    :param aptrinsic endpoint:  The Aptrinsic endpoint requesting data from.
                                Possible values include:
                                    - users
                                    - accounts
    :type aptrinsic endpoint:   string
    :param aptrinsic_filter:    The filters to be added to the query in the
                                form of "{fieldName}{operator}{fieldValue}".
                                For example, if pulling only users with first
                                name "John" and last name "Doe", the filter
                                value would be:
                                ['lastName==Doe', 'firstName==John']
    :type aptrinsic_filter:     list
    :param s3_conn_id:          The s3 connection id.
    :type s3_conn_id:           string
    :param s3_bucket:           The S3 bucket to be used to store
                                the Google Analytics data.
    :type s3_bucket:            string
    :param s3_key:              The S3 key to be used to store
                                the Hubspot data.
    :type s3_key:               string
    """

    template_fields = ('filters',
                       's3_key')

    def __init__(self,
                 aptrinsic_conn_id,
                 aptrinsic_endpoint,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 aptrinsic_filter=None,
                 page_size=1000,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.aptrinsic_conn_id = aptrinsic_conn_id
        self.endpoint = aptrinsic_endpoint
        self.filters = aptrinsic_filter
        self.page_size = page_size
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        if self.endpoint.lower() not in ('accounts', 'users'):
            raise Exception('Specified endpoint not currently supported.')

    def execute(self, context):
        """
        Execute the operator.
        This will get all the data for a particular
        Pardot model and write it to a file.
        """
        logging.info("Prepping to gather data from Aptrinsic")

        hook = AptrinsicHook(self.aptrinsic_conn_id)

        logging.info("Making request for {0} object".format(self.endpoint))

        output = self.paginate_data(hook, self.endpoint)

        self.output_manager(output)

    def paginate_data(self, g, endpoint):
        """
        This method takes care of request building and pagination.
        It retrieves 100 at a time and continues to make
        subsequent requests until it retrieves less than 100 records.
        """
        output = []
        final_payload = {'pageSize': self.page_size}
        page = 1

        if self.filters:
            filters = ';'.join([param for param in self.filters])
            final_payload['filters'] = filters

        response = g.run(endpoint, final_payload).json()

        output.extend(response.get(self.endpoint))

        logging.info('Retrieved: ' + str(len(output)))

        while len(response) == self.page_size:
            page += 1
            response = g.run(endpoint, final_payload).json()
            logging.info('Retrieved: ' + str(final_payload['per_page'] * page))
            output.extend(response.get(self.endpoint))
            final_payload['scrollId'] = response.get('scrollId')

        return output

    def output_manager(self, output):
        def flatten(record, parent_key='', sep='_'):
            flattened_record = []
            for k, v in record.items():
                new_key = parent_key + sep + k if parent_key else k
                if isinstance(v, dict):
                    flattened_record.extend(flatten(v,
                                                    new_key,
                                                    sep=sep).items())
                else:
                    flattened_record.append((new_key, v))
            return dict(flattened_record)

        output = '\n'.join([json.dumps({boa.constrict(k): v
                                        for k, v
                                        in flatten(record).items()})
                            for record in output])

        s3 = S3Hook(self.s3_conn_id)

        s3.load_string(
            string_data=output,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True
        )
