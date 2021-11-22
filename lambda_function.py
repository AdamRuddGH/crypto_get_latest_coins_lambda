from src import coin_getter
import time


def lambda_handler(event, context):   
    """
    main orchestration executable.
    Runs the job
    """
    
    return(coin_getter.orchestrate_get_coin_upload_to_s3())
    


# lambda_handler("hi","")
