#!/bin/bash

nohup /home/neo/.virtualenvs/StompyAWSScoring/bin/python /home/neo/StompyAWSScoring/update_spot_market_scores.py >> /home/neo/StompyAWSScoring/log 2>&1 &