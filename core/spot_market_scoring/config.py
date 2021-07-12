import os
import sys


class CONFIG:
    ROOT_ACC = {"id": "263078123597", "email": "admin@stompy.io", "biz": "AWS"}
    AWS_CREDENTIALS = {"aws_access_key_id": "AKIAVTKQUJDYYVD4ZYR6",
                       "aws_secret_access_key": "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"}
    SUB_CREDENTIALS = {'aws_access_key_id': 'AKIAT2QFNTBGUDLJXUZ6',
                       'aws_secret_access_key': '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'}

    DB_CREDENTIALS = {"host": "ec2-52-203-98-126.compute-1.amazonaws.com",
                      "db": "d1vk402e6g998n",
                      "user": "dtocysyyboorao",
                      "port": "5432",
                      "password": "e623b2f66fa8e79b009e1b562564a132e77236c7d2999963e20b74271dda9eca"}

    MONGODB_CONNECTION = "mongodb+srv://sophia:nMOz5qpTQ7uRXLqC@spot-market-scores.mr8yo.mongodb.net/spot-market-scores?retryWrites=true&w=majority"
    ROOT_PATH = os.path.dirname(__file__)
    # uncomment this line for local use
    # DATA_PATH = os.path.join(ROOT_PATH, 'Stompy.phase.2.data')
    DATA_PATH = "/Users/Sophiawang1228/Documents/GitHub/Stompy.phase.2.data"



    LOG_PATH = os.path.join(ROOT_PATH, 'logs')
    LOG_SIZE = 10 * 1024 * 1024
    LOG_COUNT = 100


class LOCAL(CONFIG):
    ...


class DEV(CONFIG):
    ...


class PROD(CONFIG):
    ...


conf = LOCAL

if len(sys.argv) < 2:
    conf = LOCAL
elif sys.argv[1] == 'dev':
    conf = DEV
elif sys.argv[1] == 'prod':
    conf = PROD
