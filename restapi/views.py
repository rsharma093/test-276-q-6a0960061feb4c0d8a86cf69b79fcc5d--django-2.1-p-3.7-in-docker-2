# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from datetime import timedelta
from multiprocessing import Process, Queue

import numpy as np
import pandas as pd
from django.http import HttpResponse
from rest_framework import viewsets
from rest_framework.exceptions import APIException
from rest_framework.response import Response


def index(requests):
    return HttpResponse("Hello, world. You're at Rest.")


class BadRequest(APIException):
    status_code = 400
    default_detail = "Bad Request"
    default_code = "BAD_REQUEST"


class AnalyzeLogFilesViewSet(viewsets.ViewSet):
    def create(self, request, *args, **kwargs):
        files = request.data.get("logFiles")

        # get combined dataframe of all files with parallel processing
        combineddf = self.get_combined_df(files)
        df = pd.concat(combineddf, ignore_index=True)
        if request.data.get("parallelFileProcessingCount") == 0:
            raise BadRequest(
                {"reason": "Parallel File Processing count must be greater than zero!"}
            )

        del df["id"]

        # Convert epoch timestamp to datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

        # groupby with range of 15 minutes timestamp
        formated_df = df.groupby(
            [pd.Grouper(key="timestamp", freq="15min"), "exception"]
        )["exception"].count()

        # get the count of exception for each time range
        formated_df = formated_df.to_frame("count").reset_index()

        # change timestamp column to range
        formated_df["timestamp"] = formated_df["timestamp"].apply(
            lambda x: f"{x.strftime('%H:%M')}-{(x + timedelta(minutes=15)).strftime('%H:%M')}"
        )

        # get grouped json data
        formated_data = (
            formated_df.groupby("timestamp", sort=False)
            .apply(lambda x: dict(zip(x["exception"], x["count"])))
            .to_dict()
        )

        # update response data with output format
        response = [
            {
                "timestamp": key,
                "logs": [{"exception": k, "count": v} for k, v in val.items()],
            }
            for key, val in formated_data.items()
        ]

        return Response({"response": response})

    # Function to convert files to pandas dataframes
    def get_combined_df(self, files):
        def process_files(files, queue):
            thread_output = []
            for file in files:
                file_output = pd.read_csv(
                    file, sep=" ", header=None, names=["id", "timestamp", "exception"]
                )
                thread_output.append(file_output)
            queue.put(thread_output)
            return

        threads = 10
        output = []
        queues = []
        jobs = []
        if threads > len(files):
            threads = len(files)

        file_list = np.array_split(files, threads)
        for i in range(0, threads):
            queues.append(Queue())
            job = Process(target=process_files, args=(file_list[i], queues[i]))
            jobs.append(job)
            job.start()
        for job in jobs:
            job.join()
        for queue in queues:
            output.extend(queue.get())
        return output
