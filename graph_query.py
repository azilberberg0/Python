"""
 CLI script to ...
"""

from argparse import ArgumentParser
from collections import ChainMap
from copy import deepcopy
from csv import DictReader
from csv import DictWriter
from datetime import datetime
from functools import reduce
from gzip import open as gzip_open
from itertools import product
from json import dump as json_dump
from logging import getLogger
from logging.config import dictConfig
from multiprocessing import Pool
from multiprocessing import current_process
from os import environ
from os.path import basename
from os.path import join
from os.path import splitext
from string import Template
from sys import exit as sys_exit
from time import sleep


# from omegaconf import OmegaConf
from requests import post
from requests.exceptions import ConnectionError as requests_ConnectionError
from requests.exceptions import HTTPError
from tabulate import tabulate
from yaml import dump as yaml_dump
from yaml import safe_load as yaml_load


from wiz_api.api_token import WizApiToken
from wiz_api.config import LOG_FORMAT
from wiz_api.config import LOG_LEVELS
from wiz_api.csv_file import CsvFile
from wiz_api.flat_json import FlatJson
from wiz_api.tall_json import TallJson
from wiz_api.utils import Utils


# pylint: disable=W1203


class GraphQuery:
    """
    Class to manage Wiz projects.
    """

    url = "https://api.us27.app.wiz.io/graphql"

    def __init__(self):
        """
        Create object.
        """

        self.logger = getLogger(f"{self.__class__.__name__}")
        self.utils = Utils()

    @staticmethod
    def _mp_fetch(
        log_config, config_file, request_args, save_dir, config_data, snow_data, pause
    ):
        """
        Private method to ...
        """
        dictConfig(log_config)
        mp_logger = getLogger("GraphQuery")

        sleep(pause)

        flatten_stage = (
            config_data.get("flatten_stage")
            if config_data.get("flatten_stage")
            else False
        )
        to_csv_stage = (
            config_data.get("to_csv_stage")
            if config_data.get("to_csv_stage")
            else False
        )

        tall_json = TallJson()
        flat_json = FlatJson()

        try:
            first = request_args["json"]["variables"]["first"]
            after = request_args["json"]["variables"]["after"]
            mp_logger.info(
                f"Running query: config_file={config_file}; first={first}; after={after}; pause={pause}"
            )

        except KeyError:
            pass

        response = None
        retry_count = 0
        retry_max = 3

        while retry_count < retry_max:
            try:
                response = post(**request_args)
                response.raise_for_status()
                response = response.json()

                # new here ...

                errors = response.get("errors")

                if errors:
                    for error in errors:
                        message = error.get("message")
                        mp_logger.error(
                            f"Query failed: config_file={config_file}; message={message}; error={error}"
                        )

                    raise Exception("Failed ...")

                # Nodes contain 1+ entities and the code below
                # merges node entities by nesting each entity with
                # the wiz_id and wiz_type attributes which
                # simplifies down-stream flattening and processing
                # of the results to create CSV output.

                data = response.get("data")
                assert data, "Missing required attribute: data"

                graph_search = data.get("graphSearch")
                assert graph_search, "Missing required attribute: graph_search"

                nodes = graph_search.get("nodes", None)
                assert graph_search != None, "Missing required attribute: nodes"

                save_data = []
                for node in nodes:
                    entities = node.get("entities")
                    entities = [
                        entity for entity in entities if entity
                    ]  # remove "None" or null entities

                    new_item = {}

                    for entity in entities:
                        wiz_id = entity.get("id")
                        wiz_type = entity.get("type")

                        if new_item.get(wiz_type):
                            mp_logger.warning(
                                f"Item contains an entity of the same type, skipping update: wiz_id={wiz_id}; wiz_type={wiz_type}"
                            )

                        else:
                            new_item.update({wiz_type: entity})

                    save_data.append(new_item)

                if save_data:
                    mp_logger.info(f"len(save_data): {len(save_data)}")

                    query_name = basename(config_file).replace(".yaml", "")
                    after_str = f"{after:>09}"
                    save_file = join(
                        save_dir,
                        f"{query_name}_first_{first}_after_{after_str}_tall.json.gz",
                    )

                    mp_logger.info(f"Saving data: save_file={save_file}")

                    # Write tall data to file

                    with gzip_open(save_file, "wt", encoding="UTF-8") as zip_file:
                        json_dump(save_data, zip_file)

                    mp_logger.info(f"Saved data: save_file={save_file}")

                    # Create CSV

                    field_data = config_data.get("fields")
                    assert (
                        field_data
                    ), "Missing required attribute: config_file={config_file}; attribute=fields"

                    load_data = save_data

                    save_data = TallJson.to_csv_sm(field_data, log_config, load_data, 1)

                    if config_data.get("join_with_snow") in [True, None] and snow_data:

                        # Merge with service now data

                        target_data = save_data
                        source_data = snow_data

                        join_keys = [
                            "vm_app_id=APPLICATION_ID",
                            "container_app_id=APPLICATION_ID",
                        ]

                        save_data = CsvFile.join_sm(
                            log_config, source_data, target_data, join_keys, []
                        )

                    for item in save_data:
                        item.update({"results_file": save_file})

                    save_file = save_file.replace(".json.gz", ".csv")
                    fieldnames = list(save_data[0].keys())

                    with open(save_file, "w") as file_handle:
                        writer = DictWriter(file_handle, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(save_data)

                else:
                    mp_logger.debug(
                        f"No data to save: config_file={config_file}; first={first}; after={after}; pause={pause}"
                    )

                break

            except requests_ConnectionError as error:
                mp_logger.error(
                    f"Connection error: config_file={config_file}; first={first}; after={after}; pause={pause}; error={error}"
                )

            except HTTPError as error:
                if error.errno in [502, 503, 504]:
                    mp_logger.error(
                        f"General error: config_file={config_file}; first={first}; after={after}; pause={pause}; error={error}"
                    )

                else:
                    mp_logger.error(
                        f"Wiz-API-Error: config_file={config_file}; first={first}; after={after}; pause={pause}; error={error}"
                    )

            except Exception as error:
                mp_logger.error(
                    f"Other exception: config_file={config_file}; first={first}; after={after}; pause={pause}; error={error}"
                )

            retry_count += 1
            mp_logger.error(
                f"Retrying: config_file={config_file}; first={first}; after={after}; pause={pause}; retry_max={retry_max}; retry_count={retry_count}"
            )

            sleep(pause)

        return response

    @staticmethod
    #    def _mp_fetch_by_vertex(log_config, config_file, request_args, save_dir, config_data, snow_data, pause, vertex_id, vertex_name, external_id, timestamp):
    def _mp_fetch_by_vertex(
        log_config,
        config_file,
        request_args,
        save_dir,
        config_data,
        snow_data,
        pause,
        file_tag,
        timestamp,
    ):
        """
        Private method to ...
        """
        dictConfig(log_config)
        mp_logger = getLogger("GraphQuery")

        # sleep(pause * random())

        request_count = 0
        record_count = 0
        retries = 0
        process_name = current_process().name

        response = {
            "save_dir": save_dir,
            "file_tag": file_tag,
            "retries": retries,
            "request_count": request_count,
            "record_count": record_count,
        }

        # Set proocessing flags

        flatten_stage = (
            config_data.get("flatten_stage")
            if config_data.get("flatten_stage")
            else False
        )
        to_csv_stage = (
            config_data.get("to_csv_stage")
            if config_data.get("to_csv_stage")
            else False
        )

        tall_json = TallJson()
        flat_json = FlatJson()

        # Verify attributes FIXME: Consider removing b/c it's validated in the caller

        try:
            first = request_args["json"]["variables"]["first"]
            after = request_args["json"]["variables"]["after"]

        except KeyError:
            pass

        # Set looping variables

        has_next_page = True
        item_count = 0

        while has_next_page:
            mp_logger.info(
                f"Fetching data in parallel by vertex: config_file={config_file}; process_name={process_name}; first={first}; after={after}; file_tag={file_tag}; save_dir={save_dir}"
            )

            request_count += 1
            retry_count = 0
            retry_max = 5

            while retry_count < retry_max:
                sleep(pause * (retry_count + 1))

                try:
                    api_response = post(**request_args)
                    api_response.raise_for_status()
                    api_response = api_response.json()

                    errors = api_response.get("errors")

                    if errors:
                        for error in errors:
                            message = error.get("message")
                            mp_logger.error(
                                f"Query failed: config_file={config_file}; message={message}; error={error}"
                            )

                        raise Exception("Failed ...")

                    # Nodes contain 1+ entities and the code below
                    # merges node entities by nesting each entity with
                    # the wiz_id and wiz_type attributes which
                    # simplifies down-stream flattening and processing
                    # of the results to create CSV output.

                    data = api_response.get("data")
                    assert data, "Missing required attribute: data"

                    graph_search = data.get("graphSearch")
                    assert graph_search, "Missing required attribute: graph_search"

                    nodes = graph_search.get("nodes", None)
                    assert graph_search != None, "Missing required attribute: nodes"

                    save_data = []
                    for node in nodes:
                        entities = node.get("entities")
                        entities = [
                            entity for entity in entities if entity
                        ]  # remove "None" or null entities

                        new_item = {}

                        for entity in entities:
                            wiz_id = entity.get("id")
                            wiz_type = entity.get("type")

                            if new_item.get(wiz_type):
                                mp_logger.warning(
                                    f"Item contains an entity of the same type, skipping update: wiz_id={wiz_id}; wiz_type={wiz_type}"
                                )

                            else:
                                new_item.update({wiz_type: entity})

                        save_data.append(new_item)

                    if save_data:
                        # mp_logger.error(f"PAB 1: {save_data}")
                        record_count += len(save_data)
                        request_count += 1

                        query_name = basename(config_file).replace(".yaml", "")
                        after_str = f"{after:>09}"
                        save_file = join(
                            save_dir,
                            f"{query_name}_{file_tag}_first_{first}_after_{after_str}_tall.json.gz",
                        )

                        mp_logger.info(f"Saving data: save_file={save_file}")

                        # Write tall data to file

                        with gzip_open(save_file, "wt", encoding="UTF-8") as zip_file:
                            json_dump(save_data, zip_file)

                        mp_logger.info(f"Saved data: save_file={save_file}")

                        # Create CSV

                        field_data = config_data.get("fields")
                        assert (
                            field_data
                        ), "Missing required attribute: config_file={config_file}; attribute=fields"

                        load_data = save_data

                        save_data = TallJson.to_csv_sm(
                            field_data, log_config, load_data, 1
                        )

                        # PAB - begin
                        fieldnames = list(save_data[0].keys())
                        with open("x.csv", "w") as file_handle:
                            writer = DictWriter(file_handle, fieldnames=fieldnames)
                            writer.writeheader()
                            writer.writerows(save_data)
                        # PAB - end

                        if (
                            config_data.get("join_with_snow") in [True, None]
                            and snow_data
                        ):

                            # Merge with service now data

                            target_data = save_data
                            source_data = snow_data

                            join_keys = [
                                "vm_app_id=APPLICATION_ID",
                                "container_app_id=APPLICATION_ID",
                            ]

                            save_data = CsvFile.join_sm(
                                log_config, source_data, target_data, join_keys, []
                            )

                        for item in save_data:
                            item.update({"results_file": save_file})

                        save_file = save_file.replace(".json.gz", ".csv")
                        try:
                            fieldnames = list(save_data[0].keys())
                        except Exception as error:
                            mp_logger.error(f"PAB: {error}")

                        with open(save_file, "w") as file_handle:
                            writer = DictWriter(file_handle, fieldnames=fieldnames)
                            writer.writeheader()
                            writer.writerows(save_data)

                    else:
                        mp_logger.debug(
                            f"No data to save: config_file={config_file}; first={first}; after={after}; pause={pause}"
                        )
                        has_next_page = False

                    try:
                        page_info = api_response["data"]["graphSearch"]["pageInfo"]
                        has_next_page = has_next_page & page_info.get("hasNextPage")
                        after = str(max(int(after), int(page_info["endCursor"])))

                        request_args["json"]["variables"]["after"] = after

                    except TypeError as error:
                        mp_logger.error(error)

                    break

                except requests_ConnectionError as error:
                    mp_logger.error(
                        f"Connection error: config_file={config_file}; first={first}; after={after}; pause={pause}; error={error}"
                    )

                except HTTPError as error:
                    if error.errno in [502, 503, 504]:
                        mp_logger.error(
                            f"General error: config_file={config_file}; first={first}; after={after}; pause={pause}; error={error}"
                        )

                    else:
                        mp_logger.error(
                            f"Wiz-API-Error: config_file={config_file}; first={first}; after={after}; pause={pause}; error={error}"
                        )

                except Exception as error:
                    mp_logger.error(
                        f"Other exception: config_file={config_file}; first={first}; after={after}; pause={pause}; error={error}"
                    )

                retry_count += 1
                retries += 1

                first = int(first / 2)
                request_args["json"]["variables"]["first"] = first

                if retry_count == retry_max:
                    hsa_next_page = False
                    mp_logger.error(
                        f"Giving up: config_file={config_file}; first={first}; after={after}; pause={pause}; retry_max={retry_max}; retry_count={retry_count}"
                    )

                else:
                    mp_logger.error(
                        f"Retrying: config_file={config_file}; first={first}; after={after}; pause={pause}; retry_max={retry_max}; retry_count={retry_count}"
                    )

        response["retries"] = retries
        response["request_count"] = request_count
        response["record_count"] = record_count

        return response

    def fetch_parallel(
        self,
        client_id,
        client_secret,
        config_data,
        config_file,
        first,
        log_args,
        num_pages,
        processes,
        save_dir,
    ):
        """
        Public method to ...
        """

        # Run

        after = "0"
        cur_count = 0
        has_next_page = True
        item_count = 0

        self.logger.info(
            f"Fetching data (parallel request): config_file={config_file}; first={first}; num_pages={num_pages}; processes={processes}; save_dir={save_dir}; client_id={client_id}"
        )

        wiz_api_creds = environ.get("WIZ_API_CREDS")

        wiz_api_tokens = []

        if wiz_api_creds:
            for creds in wiz_api_creds.split(" "):
                client_id = creds.split(":")[0]
                client_secret = creds.split(":")[1]
                wiz_api_token = WizApiToken()
                wiz_api_token.request(client_id, client_secret)
                wiz_api_tokens.append(wiz_api_token)

        config_data = config_data if config_data else self.utils.load_data(config_file)

        config_data = (
            config_data.get("fetch") if config_data.get("fetch") else config_data
        )

        snow_data = []

        if config_data.get("join_with_snow"):
            snow_file = config_data.get("snow_file")
            assert (
                snow_file
            ), f"Missing required attribute in configuration file: snow_file"

            with open(snow_file) as file_handle:
                reader = DictReader(file_handle)
                snow_data = list(reader)

            assert snow_data, "Invalid or missing service now data"

        payload = config_data.get("payload")
        assert payload, "Missing required attribute: payload"

        query = payload.get("query")
        assert query, "Missing required attribute: query"

        variables = payload.get("variables")
        assert variables, "Missing required attribute: variables"

        quick_val = variables.get("quick", {})
        assert quick_val != {}, "Missing required attribute: quick"
        assert (
            quick_val == False
        ), f"Invalid value, quick_mode must be false: quick={quick_val}"

        first_val = variables.get("first")
        assert first_val, f"Missing required attribute: first"

        #        request_args = {
        #            "url": "https://api.us27.app.wiz.io/graphql",
        #            "json": payload,
        #            "headers": {
        #                "Authorization": wiz_api_token.api_authorization,
        #                "Content-Type": "application/json",
        #            },
        #        }

        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")

        base_name = splitext(basename(config_file))[0]
        save_file = join(save_dir, f"{base_name}_{timestamp}_tall.json")

        self.logger.info(
            f"Saving results: config_file={config_file}; save_file={save_file}"
        )

        first = first if first else first_val
        jobs_per_chunk = processes

        while has_next_page and (num_pages is None or cur_count < num_pages):
            max_chunk = int(after) + (first * jobs_per_chunk)
            mp_args = []

            delay = 1.0
            count = 0

            for new_after in range(int(after), max_chunk, first):
                wiz_api_token = wiz_api_tokens.pop(0)

                request_args = {
                    "url": "https://api.us27.app.wiz.io/graphql",
                    "json": deepcopy(payload),
                    "headers": {
                        "Authorization": wiz_api_token.api_authorization,
                        "Content-Type": "application/json",
                    },
                }
                request_args["json"]["variables"]["first"] = first
                request_args["json"]["variables"]["after"] = str(new_after)

                wiz_api_tokens.append(wiz_api_token)

                pause = delay * count

                mp_args.append(
                    (
                        log_args,
                        config_file,
                        request_args,
                        save_dir,
                        config_data,
                        snow_data,
                        pause,
                    )
                )

                count += 1

                cur_count += 1

                if num_pages and cur_count == num_pages:
                    break

            with Pool(processes) as pool:
                results = pool.starmap(GraphQuery._mp_fetch, mp_args)

            for result in results:
                try:
                    page_info = result["data"]["graphSearch"]["pageInfo"]
                    has_next_page = has_next_page & page_info.get("hasNextPage")
                    after = str(max(int(after), int(page_info["endCursor"])))

                except TypeError as error:
                    self.logger.error(error)

        self.logger.info(
            f"Completed queries: config_file={config_file}; first={first}; num_pages={num_pages}; cur_count={cur_count}; item_count={item_count}"
        )

        return save_file, None

    def fetch_parallel_by_vertex(
        self,
        after,
        client_id,
        client_secret,
        config_data,
        config_file,
        first,
        log_args,
        num_pages,
        processes,
        save_dir,
        count=None,
    ):
        """
        Public method to ...
        """

        self.logger.info(
            f"Fetching data in parallel by vertex: after={after}; client_id={client_id}; config_file={config_file}; first={first}; num_pages={num_pages}; processes={processes}; save_dir={save_dir}; count={count}"
        )

        # Set Wiz API creds

        wiz_api_creds = environ.get("WIZ_API_CREDS")

        wiz_api_tokens = []

        if wiz_api_creds:
            for creds in wiz_api_creds.split(" "):
                client_id = creds.split(":")[0]
                client_secret = creds.split(":")[1]
                wiz_api_token = WizApiToken()
                wiz_api_token.request(client_id, client_secret)
                wiz_api_tokens.append(wiz_api_token)

        else:
            wiz_api_token = WizApiToken()
            wiz_api_token.request(client_id, client_secret)
            wiz_api_tokens.append(wiz_api_token)

        # Load config data

        config_data = config_data if config_data else self.utils.load_data(config_file)

        config_data = (
            config_data.get("fetch") if config_data.get("fetch") else config_data
        )

        payload = config_data.get("payload")
        assert payload, "Missing required attribute: payload"

        query = payload.get("query")
        assert query, "Missing required attribute: query"

        variables = payload.get("variables")
        assert variables, "Missing required attribute: variables"

        variables_subs = config_data.get("variables_subs")

        quick_val = variables.get("quick", {})
        assert quick_val != {}, "Missing required attribute: quick"
        assert (
            quick_val == False
        ), f"Invalid value, quick_mode must be false: quick={quick_val}"

        first_val = variables.get("first")
        assert first_val, f"Missing required attribute: first"

        # Load ownership data (service now/snow)

        snow_data = []

        if config_data.get("join_with_snow") and config_data.get("join_with_snow") == True:
            snow_file = config_data.get("snow_file")
            assert (
                snow_file
            ), f"Missing required attribute in configuration file: snow_file"

            with open(snow_file) as file_handle:
                reader = DictReader(file_handle)
                snow_data = list(reader)

            assert snow_data, "Invalid or missing service now data"

        # print(snow_data)

        # Set common variables

        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")

        pause = processes * 0.1 * 2  # (seconds) processes * 10 TPS * 2

        first = first if first else first_val
        after = (
            after if after else 0
        )  # FIXME: expose after as an argument and parameter

        jobs_per_chunk = processes

        # Load variable substitution

        subs_data = []
        for item in variables_subs:
            key = item.get("key")
            values = item.get("values")
            values_file = item.get("values_file")
            values_field = item.get("values_field")

            if values_file and values_field:
                load_data = self.utils.load_data(values_file)
                new_items = [{key: f"'{item.get(values_field)}'"} for item in load_data]
                subs_data.append(new_items)

            elif values:
                new_items = [{key: f"'{value}'"} for value in values]
                subs_data.append(new_items)

            elif key == "hex_1_char":
                values = [f"{i:01x}" for i in range(16)]
                new_items = [{key: f"'{value}'"} for value in values]
                subs_data.append(new_items)

            elif key == "hex_2_char":
                values = [f"{i:02x}" for i in range(16 * 16)]
                new_items = [{key: f"'{value}'"} for value in values]
                subs_data.append(new_items)

            elif key == "hex_3_char":
                values = [f"{i:03x}" for i in range(16 * 16 * 16)]
                new_items = [{key: f"'{value}'"} for value in values]
                subs_data.append(new_items)

            else:
                self.logger.warning(f"Unknown substitution key: key={key}")

        #        print(subs_data)

        subs = []

        for item in product(*subs_data):
            subs.append(dict(ChainMap(*item)))
        #            print(dict(ChainMap(*item)))

        subs = subs[:count]
        # subs = subs[:num_pages]

        # Create request args

        mp_args = []

        variables = yaml_dump(payload["variables"])

        for sub_item in subs:
            wiz_api_token = wiz_api_tokens.pop(0)

            new_item = {
                "url": "https://api.us27.app.wiz.io/graphql",
                "json": deepcopy(payload),
                "headers": {
                    "Authorization": wiz_api_token.api_authorization,
                    "Content-Type": "application/json",
                },
            }
            new_item["json"]["variables"]["first"] = first
            new_item["json"]["variables"]["after"] = str(after)

            variables = yaml_dump(new_item["json"]["variables"])
            template = Template(variables)
            #            print(type(variables))
            #            print("variables:", variables)
            variables = template.substitute(sub_item)
            variables = yaml_load(variables)
            new_item["json"]["variables"] = variables

            wiz_api_tokens.append(wiz_api_token)

            # print("_".join([str(item) for item in sub_item.values()]))
            file_tag = "_".join([item for item in sub_item.values()])
            file_tag = file_tag.replace("'", "")
            #            print(file_tag); exit()

            mp_args.append(
                (
                    log_args,
                    config_file,
                    new_item,
                    save_dir,
                    config_data,
                    snow_data,
                    pause,
                    file_tag,
                    timestamp,
                )
            )
        #            print(yaml_dump(new_item)); exit()
        #            request_args_data.append((new_item, file_tag))
        #            print(sub_item)
        #            print(yaml_dump(new_item))

        #        exit()

        # print(json_dumps(request_args_data, indent=4, sort_keys=True))

        # Run in parallel

        #        mp_args = []
        #        for request_args, file_tag in request_args_data:
        ##            vertex = request_args.get("json").get("variables")
        ##            for vertex_key in vertex_keys:
        ##                vertex = vertex.get(vertex_key)
        #
        ##            mp_args.append((log_args, config_file, request_args, save_dir, config_data, snow_data, pause, vertex_id, vertex_name, external_id, timestamp))
        #            mp_args.append((log_args, config_file, request_args, save_dir, config_data, snow_data, pause, file_tag, timestamp))

        #        mp_args = mp_args[:4]

        with Pool(processes) as pool:
            results = pool.starmap(GraphQuery._mp_fetch_by_vertex, mp_args)

        print(tabulate(results, headers="keys", tablefmt="pipe"))

        return None, None

    def fetch_serial(
        self,
        config_file,
        save_dir,
        client_id,
        client_secret,
        config_data=None,
        first=None,
        num_pages=None,
    ):
        """
        Public method to ...
        """

        self.logger.info(
            f"Fetching data (serial request): config_file={config_file}; first={first}; num_pages={num_pages}; save_dir={save_dir}; client_id={client_id}"
        )

        wiz_api_token = WizApiToken()
        wiz_api_token.request(client_id, client_secret)

        config_data = config_data if config_data else self.utils.load_data(config_file)

        config_data = (
            config_data.get("fetch") if config_data.get("fetch") else config_data
        )

        payload = config_data.get("payload")
        prefix_keys_for_nodes = config_data.get("prefix_keys_for_nodes")
        prefix_keys_for_page_info = config_data.get("prefix_keys_for_page_info")

        variables = payload.get("variables")

        assert payload, "Missing required configuration key: payload"
        assert (
            prefix_keys_for_nodes
        ), "Missing required configuration key: prefix_keys_for_nodes"
        #        assert prefix_keys_for_page_info, "Missing required configuration key: prefix_keys_for_page_info"
        assert variables, "Missing required configuration key: variables"

        request_args = {
            "url": "https://api.us27.app.wiz.io/graphql",
            "json": payload,
            "headers": {
                "Authorization": wiz_api_token.api_authorization,
                "Content-Type": "application/json",
            },
        }

        if first:
            request_args["json"]["variables"]["first"] = first
        else:
            first = request_args["json"]["variables"]["first"]

        after = None
        has_next_page = True
        page_count = 1
        page_info = None
        save_data = []
        save_response = {}

        while has_next_page:
            self.logger.info(
                f"Fetching data (serial request): config_file={config_file}; save_dir={save_dir}; client_id={client_id}; first={first}; after={after}"
            )

            try:
                response = post(**request_args)
                response.raise_for_status()

                save_response = response.json()

            except requests_ConnectionError as err:
                self.logger.error(err)
                raise

            except HTTPError as err:
                if err.errno in [502, 503, 504]:
                    self.logger.error(err)
                    self.logger.error("Retry")

                else:
                    self.logger.error(f"Wiz-API-Error: {err}")

                raise

            except Exception as err:
                self.logger.error(err)
                raise

            errors = save_response.get("errors")

            if errors:
                for error in errors:
                    message = error.get("message")
                    log_message = f"Query failed: config_file={config_file}; save_dir={save_dir}; client_id={client_id}; message={message}"
                    self.logger.error(log_message)
                    raise Exception(log_message)

            nodes = reduce(
                lambda item, key: item.get(key, {}),
                prefix_keys_for_nodes,
                save_response,
            )
            assert (
                nodes
            ), f"Missing required attribute in response: attribute=nodes; prefix_keys={prefix_keys_for_nodes}"

            if prefix_keys_for_page_info:
                page_info = reduce(
                    lambda item, key: item.get(key, {}),
                    prefix_keys_for_page_info,
                    save_response,
                )
                assert (
                    page_info
                ), f"Missing required attribute in response: attribute=page_info; prefix_keys={prefix_keys_for_page_info}"

            save_data.extend(nodes)

            has_next_page = page_info.get("hasNextPage") if page_info else False
            end_cursor = page_info.get("endCursor") if page_info else False

            if has_next_page and end_cursor:
                request_args["json"]["variables"]["after"] = end_cursor
                after = end_cursor

            else:
                has_next_page = False

            if num_pages and page_count == num_pages:
                break
            else:
                page_count += 1

        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")

        base_name = splitext(basename(config_file))[0]
        save_file = join(save_dir, f"{base_name}_{timestamp}_tall.json")

        self.utils.save_data(save_data, save_file)

        return save_file, save_data


def main():
    ##################################################################
    # Parse command line arguments
    ##################################################################

    parser = ArgumentParser(add_help=True)

    parser.add_argument(
        "-L",
        "--log_level",
        default="INFO",
        choices=LOG_LEVELS,
        help="Log level",
    )
    parser.add_argument("-s", "--save_dir", default="data", help="Save directory")

    sub_parsers = parser.add_subparsers(help="Sub-command help", dest="command")

    ##################################################################
    # fetch-serial
    ##################################################################

    parser_fetch_serial = sub_parsers.add_parser(
        "fetch-serial", help="Run graph query and save"
    )
    parser_fetch_serial.add_argument(
        "-c", "--config_file", required=True, help="Search configuration file"
    )

    ##################################################################
    # fetch-parallel
    ##################################################################

    parser_fetch_parallel = sub_parsers.add_parser(
        "fetch-parallel", help="Run graph query and save"
    )
    parser_fetch_parallel.add_argument(
        "-c", "--config_file", required=True, help="Search configuration file"
    )
    parser_fetch_parallel.add_argument(
        "-f",
        "--first",
        type=int,
        default=None,
        help="Retrieve 'first' N of records per request (default is the 'first' value in variables, maximum is 1000)",
    )
    parser_fetch_parallel.add_argument(
        "-n",
        "--num_pages",
        type=int,
        default=None,
        help="Number of pages to retrieve (default is all pages)",
    )
    parser_fetch_parallel.add_argument(
        "-p",
        "--processes",
        default=10,
        type=int,
        help="Number of parallel processes (default 10)",
    )

    ##################################################################
    # Parse arguments
    ##################################################################

    args = parser.parse_args()

    ##################################################################
    # Configure logger and initialize variables
    ##################################################################

    log_args = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"default": {"class": "logging.Formatter", "format": LOG_FORMAT}},
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": args.log_level,
            },
        },
        "root": {"level": "DEBUG", "handlers": ["console"]},
    }

    dictConfig(log_args)
    logger = getLogger()

    ##################################################################
    # Do the work
    ##################################################################

    wiz_client_id = environ.get("WIZ_CLIENT_ID")
    wiz_client_secret = environ.get("WIZ_CLIENT_SECRET")

    assert wiz_client_id, "Missing required environment variable: WIZ_CLIENT_ID"
    assert wiz_client_secret, "Missing required environment variable: WIZ_CLIENT_SECRET"

    ##################################################################
    # Do the work
    ##################################################################

    graph_query = GraphQuery()

    if args.command == "fetch-serial":
        graph_query.fetch_serial(
            args.config_file,
            args.save_dir,
            wiz_client_id,
            wiz_client_secret,
        )

    elif args.command == "fetch-parallel":
        call_args = {
            "client_id": wiz_client_id,
            "client_secret": wiz_client_secret,
            "config_data": None,
            "config_file": args.config_file,
            "first": args.first,
            "log_args": log_args,
            "num_pages": args.num_pages,
            "processes": args.processes,
            "save_dir": args.save_dir,
        }

        graph_query.fetch_parallel(**call_args)

    else:
        parser.print_help()
        sys_exit(1)


if __name__ == "__main__":
    main()
