FROM public.ecr.aws/lambda/python:3.10

RUN pip3 install --upgrade pip \
    && yum install gcc gcc-c++ -y

RUN pip3 install pandas==2.0.3 duckdb==0.8.1 --target "${LAMBDA_TASK_ROOT}"

RUN python3 -c "import duckdb; duckdb.query('INSTALL httpfs;');"

# RUN echo ${LAMBDA_TASK_ROOT}
COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

COPY app.py ${LAMBDA_TASK_ROOT}

CMD [ "app.lambdaHandler" ]