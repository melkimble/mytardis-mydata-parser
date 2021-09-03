import requests
from . import settings
from .logger_settings import api_logger


class DatasetFilter:
    def __init__(self, model_name, params_dict, dataset_filter_filename,
                 rclone_filter_file_dir=settings.RCLONE_FILTER_FILE_DIR,
                 mytardis_api_url=settings.MYTARDIS_API_URL,
                 mytardis_api_user=settings.MYTARDIS_API_USER,
                 mytardis_api_password=settings.MYTARDIS_API_PASSWORD,):
        self.model_name = model_name
        self.params_dict = params_dict
        self.dataset_filter_filename = dataset_filter_filename
        self.rclone_filter_file_dir = rclone_filter_file_dir
        self.mytardis_api_url = mytardis_api_url
        self.mytardis_api_user = mytardis_api_user
        self.mytardis_api_password = mytardis_api_password

    def get_urn(self, model_name, params_dict):
        try:
            param_fmt = '&'.join([f'{key}={value}' for key, value in params_dict.items()])
            urn = model_name+"/?"+param_fmt
            api_logger.info('set_params: [' + str(urn) + ']')
            return urn
        except Exception as err:
            raise RuntimeError("** Error: set_params Failed (" + str(err) + ")")

    def get_uri(self, url, urn):
        try:
            return url + urn
        except Exception as err:
            raise RuntimeError("** Error: get_uri Failed (" + str(err) + ")")

    def get_response(self, uri, mytardis_api_user, mytardis_api_password):
        try:
            api_logger.info('get_response: [' + str(uri) + ']')
            response = requests.get(uri, auth=(mytardis_api_user, mytardis_api_password))
            api_logger.info('response: [' + str(response) + ']')
            # checks if response is not 4xx or 5xx
            if response.ok:
                data_json = response.json()
                # api_logger.info('data_json: [' + str(data_json) + ']')
                return data_json, response.status_code
            else:
                return False, response.status_code

        except Exception as err:
            raise RuntimeError("** Error: get_response Failed (" + str(err) + ")")

    def make_filter_request(self):
        try:
            params_dict = self.params_dict
            model_name = self.model_name
            mytardis_api_url = self.mytardis_api_url
            mytardis_api_user = self.mytardis_api_user
            mytardis_api_password = self.mytardis_api_password

            urn = self.get_urn(model_name, params_dict)
            uri = self.get_uri(mytardis_api_url, urn)
            data_json, response_code = self.get_response(uri, mytardis_api_user, mytardis_api_password)
            if data_json:
                return data_json, response_code
            else:
                return False, response_code
        except Exception as err:
            raise RuntimeError("** Error: make_filter_request Failed (" + str(err) + ")")

    def write_filter_file(self):
        try:
            dataset_filter_filename = self.dataset_filter_filename
            rclone_filter_file_dir = self.rclone_filter_file_dir
            output_filter_file = rclone_filter_file_dir+dataset_filter_filename+".txt"

            api_logger.info('write_filter_file: [' + str(output_filter_file) + ']')

            data_json, response_code = self.make_filter_request()
            if data_json:
                filter_file = open(output_filter_file, 'w')
                for each in data_json['objects']:
                    dataset_description = each['description']
                    dataset_description_fmt = "+ **/" + dataset_description + "*/**"
                    filter_file.write(dataset_description_fmt+"\n")
                # last line "exclude all else"
                filter_file.write("- **")
                filter_file.close()
            else:
                api_logger.info('[FAIL] Response: ['+str(response_code)+']')
        except Exception as err:
            raise RuntimeError("** Error: write_filter_file Failed (" + str(err) + ")")
