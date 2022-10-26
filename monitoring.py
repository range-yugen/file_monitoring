from util import *
import great_expectations as ge

def monitor_file():
    env = "prod"
    bucket = bucket_name
    region_name = region_anme

    access_key = "xxxxxxxxxxxx"

    s3 = get_boto3_client(env, "s3", access_key, region_name)
    s3_resource = boto3.resource("s3")
    
    # to write great expectation result so that we can see them
    monitor_result = {}

    filename = "monitor_pass1.csv"
    pass1_monitor = read_file(s3, bucket, filename, env)
    
    ## to use great-expectation library on file
    pass1_monitor = ge.from_pandas(pass1_monitor)

    # here we are putting condition that if number of records in pass1 output increase from 50000 then we don't get any success
    pass1_total_records_monitor = (
        pass1_monitor.expect_column_values_to_be_between(
            "total records in pass1", 0, 50000
        )
    )
    print(pass1_total_records_monitor)
    ## adding result in monitor_result dict
    monitor_result[
        "pass1_total_records_monitor"
    ] = ge.core.serializer.convert_to_json_serializable(pass1_total_records_monitor)

    ## we also monitor max size cap because if max size cap changes then according to that total replen qty will also change.
    pass1_max_size_cap_monitor = (
        pass1_monitor.expect_column_distinct_values_to_be_in_set(
            "max size cap", [2]
        )
    )
    print(pass1_max_size_cap_monitor)
    monitor_result[
        "pass1_max_size_cap_monitor"
    ] = ge.core.serializer.convert_to_json_serializable(pass1_max_size_cap_monitor)

    ## pass1_wh_stock_greater_than_total_replen_monitor (total wh_Stock >= total replen qty) if this metric having value 1 then it is good
    ## but if this is having 0 then it is not possible so that we can assume there is something wrong.
    pass1_wh_stock_greater_than_total_replen_monitor = (
        pass1_monitor.expect_column_distinct_values_to_be_in_set(
            "wh stock greater than total replen flag", [1]
        )
    )
    print(pass1_wh_stock_greater_than_total_replen_monitor)
    monitor_result[
        "pass1_wh_stock_greater_than_total_replen_monitor"
    ] = ge.core.serializer.convert_to_json_serializable(
        pass1_wh_stock_greater_than_total_replen_monitor
    )

    dir_name = "ars_monitor/"
    pass1_file_path = dir_name + "monitor_result.json"
   
    ## writing our monitor result
    write_file_to_json(bucket, s3, env, pass1_file_path, monitor_result, dir_name)

monitor_file()





