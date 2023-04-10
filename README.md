<br/>

# 프로젝트 개요

해당 프로젝트는 데이터 엔지니어링 분야에서 주로 쓰이는 서비스(AWS, Airflow, Spark, ...)를 활용하여 데이터를 처리하는 프로세스를 구현하는데 초점을 둔다.
<br/>
<br/>
<br/>

# 시나리오
<br/>

> **TLC Taxi Record Data<br/>**
> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page <br/><br/>
> TLC는 뉴욕의 택시와 모빌리티를 관리하는 기관이다.<br/>
> TLC는 2009년부터 Trip Record 데이터를 취합했으며, 매년 수십 GB에 달하는 방대한 데이터를 무료로 제공한다.<br/>
> 포함된 정보는 승하차 시간/위치, 이동거리, 승객 수, 요금 등의 정보이다.
<br/>
 
TLC Taxi Record 데이터셋 중 우버(Uber), 리프트(Lyft) 같은 차량공유서비스의 Trip Record 데이터인 **High Volume For-Hire Vehicle Trip Records (HVFHV)** 데이터셋을 활용하여 다음과 같은 분석 목적을 기반으로 데이터 엔지니어링을 하고자 한다.
<br/>
<br/>

**서비스 품질 분석**<br/>
경쟁사와 자사의 ETA(콜 요청으로부터 요청장소까지 Taxi가 도착하는 경과시간)를 비교 분석하는 데이터를 생성한다.

**ML용 데이터 서빙**<br/>
서비스 사용자에게 예상 도착시간 정보를 제공할 목적으로 소요시간(ETA)을 예측하기 위해 ML 학습용 데이터를 생성한다.

**수요 분석**<br/>
- 시장 점유율 분석<br/>
경쟁사와 자사의 서비스를 얼마나 사용하는지 비교 분석하는 데이터를 생성한다.

- 인기 지역 분석<br/>
택시 수요가 많은 인기 지역을 분석하기 위한 데이터를 생성한다.<br/> 
더 나아가 인기 급상승 지역을 분석하여 떠오르는 지역이 어디인지 파악한다.<br/>
이를 통해 수요를 예측하고, 배차 할당을 조절한다.
<br/>
<br/>

이런 분석 결과는 비즈니스 의사결정을 지원하거나 바로 서비스에 적용된다. 그렇기 때문에 일회성으로 분석되기보다는 지속적으로 분석되어져야 한다.<br/>
즉 분석 결과가 지속적으로 누적되며 시간 경과에 따른 트렌드(추이)를 만들고, 이를 모니터링함으로서 비즈니스 전략을 수립하거나 서비스를 고도화시킨다.<br/>
데이터 엔지니어링 측면에서의 과제는 이런 가공된 데이터를 지속적으로 서빙하는 것이다. 따라서 해당 프로젝트는 일회성이 아닌 자동화를 고려한 설계를 바탕으로 구현하였다.<br/>
또한 고가용성(HA), 내결함성(FT), 탄력성(Elastic) 등의 엔지니어링 전략에 대해 깊게 다루진 않지만, 이런 특성을 잘 갖추어 표준처럼 자리잡은 서비스들을 활용하여 구현하였다.
<br/>
<br/>
<br/>

# 아키텍처
![image](https://user-images.githubusercontent.com/22818292/229718534-f9494483-ac64-4ffd-bd4f-b4f82f6d6e14.png)

## 서비스 및 용도

### AWS - RDS (MySQL)
Airflow 메타데이터 DB 및 수집할 데이터셋에 대한 메타데이터 제공, 커스텀 로깅 용도

RDS는 다양한 DB 엔진을 지원하며, 인스턴스 유지 관리에 소요되는 시간을 줄여주고, 읽기 전용 복제본으로 트래픽 부하를 줄이는 등 다양한 이점이 있어 많이 사용한다.

Airflow 메타 DB 외에 데이터셋에 대한 메타정보와 데이터셋 수집 시 별도로 로그를 저장하는 `tlc_taxi`라는 DB를 두었다.

![image](https://user-images.githubusercontent.com/22818292/230822983-ddcf92a2-4770-4607-a49f-d03c6e4810e3.png)

**`dataset_meta`**

가져올 데이터셋에 대한 ID를 부여한 Master 테이블이다.

![image](https://user-images.githubusercontent.com/22818292/230822811-b91c61b0-8455-41f5-99a5-56f9091bd286.png)

**`dataset_log`**

데이터셋 수집 시 Custom log를 저장하는 테이블이다.

![image](https://user-images.githubusercontent.com/22818292/230822649-95017a11-3ae5-40b7-a0a5-d928f1ba8e52.png)

<br/>

### AWS - S3
수집한 데이터셋 저장, 가공한 데이터셋 저장 용도

데이터 및 요청을 파이셔닝하는데 유용하며, DataLake로 활용을 많이 한다.


### AWS - EC2
Airflow Multi Node Cluster 구성을 위해 사용

Airflow 관리형 서비스인 MWAA를 쓸 수 있지만, Airflow Cluster에 대한 이해도를 높이기 위해 여러 대의 EC2로 날 것으로 구성한다.
  
### AWS - EMR
Spark 사용 목적

Spark Cluster까지 EC2 날 것으로 구성하려면 피로도가 높다. EMR은 서비스 하나로 Spark, Hadoop, YARN, Jupyter Notebook 같은 다양한 Application을 간편하게 구성할 수 있도록 도와준다.

또한 Application에서 제공하는 모니터링 UI를 제공하고, S3에 로깅도 해주기 때문에 debugging도 용이하다. 

### Airflow
데이터 수집 프로세스와 분석 프로세스에 대한 Workflow 정의 및 스케줄링에 사용한다.

이 각 프로세스는 각 DAG와 매핑된다.

- 데이터 수집 프로세스 = 데이터 수집 DAG
- 데이터 분석 프로세스 = 데이터 가공 및 분석 DAG

### Spark
S3에 적재된 대량의 데이터셋을 분석하고 가공하는데 사용한다.

MPP DB인 Redshift에 저장하고 시각화까지 하는 파이프라인을 만들 수 있지만, 프로젝트의 범위를 S3에 추출하는 것으로 제한한다.

### GitHub
Airflow DAG, Spark script에 대한 저장소 및 배포 자동화를 위해 사용한다.
<br/>
<br/>
<br/>

## 프로세스 개요
자세한 로직에 대한 설명은 아래에서 하고 간략하게 프로세스가 어떻게 작동하는지 알아본다.

### DAG 개발 및 배포 프로세스
1. Local에서 DAG Script 작성 후 GitHub에 Push를 하게 되면 GitHub Actions을 활용하여 Airflow Cluster Node들에 DAG가 배포된다.

### 데이터 수집 프로세스
1. RDS (MySQL)에서 수집할 데이터셋에 대한 링크 정보를 가져온다.
2. 수집한 데이터셋을 S3에 저장한다.

### 데이터 분석 프로세스
1. EMR Cluster를 생성하고 EMR의 Spark submit Step을 실행한다.
2. 실행된 Spark Application은 데이터 수집 프로세스에서 S3에 저장한 데이터셋을 Source로 읽어들여 분석하고 가공한다.
3. 가공된 데이터셋을 S3에 저장한다.
<br/>
<br/>
<br/>

## Cluster 아키텍처
### Airflow Cluster
Airflow Cluster는 아래와 같은 EC2 Node들로 구성된다.<br/>
무거운 작업은 EMR로 위임할 것이기 때문에 Airflow Cluster 사양이 굳이 높을 필요는 없다.<br/>
비용 및 리소스 낭비를 줄이기 위해 높은 사양은 사용하지 않는다.<br/>
<br/>

![image](https://user-images.githubusercontent.com/22818292/230561284-e3cd3750-e8fa-4b41-ae2f-7d021cc8c7aa.png)

![image](https://user-images.githubusercontent.com/22818292/230561536-4a0e6804-dcb2-4a94-ae42-a139ef0cf6d7.png)
<br/>
<br/>

Airflow Cluster를 이루는 Component들을 좀 더 자세히 살펴보면 다음과 같다.<br/>
내용 편의를 위해 DAG 개발 및 배포 프로세스에 대한 아키텍처도 함께 설명한다.

![image](https://user-images.githubusercontent.com/22818292/230565352-2894ce92-4a1e-4dd8-9e8f-4c70b72d2a37.png)

**airflow-primary** : Airflow의 주요 프로세스들이 해당 Node에 위치해 있다.
-  Scheduler : DAG와 Task를 모니터링하고, 예약된 DAG를 Trigger하고, 실행할 Task를 Executor (Queue)에 제출하는 프로세스
-  Webserver : Airflow Web UI
-  Executor : 그림에 보이지 않는데 Executor Logic은 Scheduler 프로세스 안에서 실행된다. `CeleryExecutor`로 구성하였으며, Celery Worker에 Task 실행을 Push한다. 
-  Celery Flower : Celery Worker를 모니터링할 수 있는 Web UI<br/><br/>
  ![image](https://user-images.githubusercontent.com/22818292/230565853-30f5cb3a-5927-449c-a6d0-c15759608041.png)
  <br/>

**airflow-borker** : `CeleryExecutor` 사용 시 Broker와 Result backend 설정이 필요하다. 이 역할로 Redis를 사용한다. 
-  Broker : Task Queue로, 별다른 설정없이 `default` Queue를 사용
-  Result backend : Task 상태를 저장한다.

**airflow-worker\*** : 할당된 Task를 실행한다.

**GitHub Actions Runner** : `CeleryExecutor` 사용 시, Celery Worker가 DAG 폴더에 접근할 수 있어야 한다. 그리고 Node들이 동기화 된 DAG를 봐라봐야 한다. <br/>
예를 들어 Primary Node가 바라보는 DAG가 최신화 되어 있고, Worker Node가 바라보는 DAG는 최신화가 안 되어 있다면 Web UI에서는 최신화 된 DAG Logic을 볼 수 있지만, Task 실행 시 최신화 된 Logic을 실행하지 못한다. <br/>
따라서 DAG 개발 및 배포의 편의성 측면과 DAG Sync 측면에서 GitHub Repository와 GitHub Actions를 사용한다. Push가 일어났을 때 Airflow Cluster의 모든 Node들의 DAG 폴더를 자동으로 Update 및 Sync 할 수 있도록 하기 위해서 각 Node에 GitHub Actions Self-hosted Runner를 설치하고 구성한다.

**Statsd Exporter** : Airflow Metric을 제공하는 프로세스이다. 이후에 Prometheus와 Grafana와 연동할 예정이다.

**RDS** : RDS (MySQL)의 `airflow` DB를 Airflow 메타데이터를 저장하는 DB로 사용한다.

![image](https://user-images.githubusercontent.com/22818292/229800380-274fff08-cf35-470c-9dab-36d25c66d86a.png)
<br/>
<br/>

### EMR Cluster
EMR Cluster는 Spark + YARN(default)만 사용할 예정이다.<br/>
Cluster Resource 내에서 데이터셋의 크기에 따라 탄력적으로 조절하는 방식으로 운영하기 위해, 기본 사양은 Core Node 2대와 `m5.xlarge` 유형으로 구성한다.
<br/>

![image](https://user-images.githubusercontent.com/22818292/230817404-a314a541-4598-4c96-981e-06fc7b06fa8c.png)

<br/>

```yaml
JOB_FLOW_OVERRIDES = {
    "Name": "PySpark Cluster",
    "LogUri": "s3://emr--log/",
    "ReleaseLabel": "emr-6.10.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "EmrManagedMasterSecurityGroup": "sg-0a8997b0ae4e90d07",
        "EmrManagedSlaveSecurityGroup": "sg-055cef9cc6cc12658",
        "Ec2KeyName": "airflow",
        "Ec2SubnetId": "subnet-8cf1eee4",
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core Node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "Configurations": [
        {
            "Classification": "yarn-site",
            "Properties": {
                "yarn.resourcemanager.am.max-attempts": "1"
            }
        },
        {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            }
        }
    ]
}
```
<br/>

# 파일 구조

```
airflow-spark-emr
├── .github
│   └── workflows
│       └── checkout.yaml
├── airflow_dags
│   ├── analyze_tlc_taxi_record.py
│   └── download_tlc_taxi_record.py
└── spark_scripts
    ├── analyze_data.py
    └── preprocess_data.py
```

- `checkout.yaml` : Airflow DAGs를 Airflow Cluster에, Spark scripts를 S3에 deploy하는 프로세스를 정의한 문서
- `airflow_dags` : Airflow DAGs를 담은 폴더

  ![image](https://user-images.githubusercontent.com/22818292/230822139-f8ff50eb-f563-422e-a490-5f576e41db69.png)
  - `analyze_tlc_taxi_record.py` :  EMR Cluster를 생성하고, Spark를 실행하는 DAG
  
  - `download_tlc_taxi_Record.py` : TLC Taxi Record 데이터를 수집해서 S3에 저장하는 DAG
 
- `spark_scripts` : Spark에서 실행할 Scripts를 담은 폴더

  - `analyze_data.py` : `preprocess_data.py` 로직을 통해 추출된 데이터를 기반으로 다양한 분석용 데이터를 생성한다.
  
  - `preprocess_data.py` : Raw 데이터를 분석하기 위해 전처리하는 Script <br/>
   이 과정을 통해 출력되는 데이터는 분석을 위해 공통으로 쓰이는 데이터가 된다.<br/>
   전처리 과정에서는 데이터량을 줄이기 위해 불필요한 데이터는 지우고, 분석을 위해 필요한 데이터를 생성한다.
   
<br/>
<br/>

# 전체적인 흐름 설명

## 데이터 수집 프로세스

### 수집 로직

수집 DAG의 Task를 살펴보면 다음과 같다.

<br/>

**download_tlc_taxi_record.py**
```python
    get_latest_dataset_id = get_latest_dataset_id() # 1
    get_urls = get_url(num=2) # 2

    get_latest_dataset_id >> get_urls

    fetch.expand(url=get_urls) # 3
```

1. `dataset_log` 로그 테이블에서 마지막으로 처리된 데이터셋 ID를 가져온다.
2. `dataset_meta` 메타 테이블에서 이번 실행에 수집할 데이터셋의 링크를 가져온다.<br/>
이번 실행에 수집할 데이터셋 링크는 마지막에 실행됐던 데이터셋 ID 이후 ID를 가져온다.<br/>
`num` 파라미터는 몇 개의 데이터셋을 수집할 지 지정한다.<br/>
3. Dynamic Task Mapping 개념을 이용해서 데이터를 수집한다.<br/>
Dynamic Task Mapping은 Runtime 때 정의된 만큼의 Task를 생성한다.<br/>
데이터셋에 대한 수집 프로세스를 병렬로 처리하기 위하여 사용하였다.

<br/>

예를들어

`2019-03` 데이터셋까지 수집된 상태이고,

현재 설정이 

- `num=2`이면, `2019-04`, `2019-05` 데이터
- `num=3`이면,  `2019-04`, `2019-05`, `2019-06` 데이터

가 현재 실행에서 **병렬**로 수집된다.

`num=2`
![image](https://user-images.githubusercontent.com/22818292/230826943-c962530f-3939-46bf-8cf7-87e075bf545e.png)
 `num=3`
![image](https://user-images.githubusercontent.com/22818292/230828233-5b87564e-650d-4e40-9f16-77bf1c83aa2a.png)

<br/>
<br/>

### S3
TLC Taxi Record 데이터를 S3의 `source` 폴더에 연도 파티션 단위로 저장한다.

데이터셋은 `parquet` 포맷으로, 하나당 보통 `500MB`는 되고, 연도당 `6GB`가 넘는다. (parquet는 압축률이 좋기 때문에 csv로 치면 최소 수 십 `GB` 될 것이다.)

![image](https://user-images.githubusercontent.com/22818292/230821705-2ae3a083-e6a5-4953-9dbd-35e358113f94.png)

<br/>

## 데이터 분석 프로세스

## EMR Cluster 설정 로직
1. EMR Cluster를 생성하고, Pyspark Submit Step을 추가한다.
2. Step을 Sensor가 모니터링한다.
3. 모든 Step이 완료되면 Cluster를 종료한다.

**analyze_tlc_taxi_record.py**
```python
create_job_flow = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    add_steps = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id=create_job_flow.output,
        steps=SPARK_STEPS,
        wait_for_completion=True,
    )

    check_job_flow = EmrJobFlowSensor(
        task_id="check_job_flow",
        job_flow_id=create_job_flow.output
    )

    remove_cluster = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id=create_job_flow.output
    )

create_job_flow >> add_steps >> check_job_flow >> remove_cluster
```

## Spark Submit 로직
Spark Submit 로직은 2 부분으로 구성된다.


```yaml
SPARK_STEPS = [
    {
        "Name": "Preprocess TLC Taxi Record",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://tlc-taxi/scripts/preprocess_data.py",
                "--src",
                "s3://tlc-taxi/source/2019/",
                "--output",
                "s3://tlc-taxi/output/preprocess/",
            ]
        }
    },
    {
        "Name": "Analyze preprocessed TLC Taxi Record",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://tlc-taxi/scripts/analyze_data.py",
                "--src",
                "s3://tlc-taxi/output/preprocess/",
                "--output",
                "s3://tlc-taxi/output/analyze/",
            ]
        }
    },
]
```

- `preprocess_data.py` Script를 이용하여, S3 `source` 폴더에 있는 데이터를 연도 파티션 단위로 읽어서 전처리 과정을 진행한다.<br/>
전처리된 데이터는 `output/preprocess/` 경로에 Export된다.

- `analyze_data.py` Script를 이용하여, 전처리된 데이터를 기반으로 다양한 분석 데이터를 `output/anlayze/` 경로에 Export한다.

<br/>

## 최종 결과
최종 데이터는 `output/anlayze/` 경로에 저장된다.

도입부 시나리오 섹션에서 살펴본 것과 같은 분석 데이터를 제공한다.

![image](https://user-images.githubusercontent.com/22818292/230843780-ead6d5d3-8df3-49a1-bd5e-cdeaeeb02ecb.png)

### `avg_elpased_by_month`
경쟁사와 자사의 ETA(콜 요청으로부터 요청장소까지 Taxi가 도착하는 경과시간)를 비교 분석하는 데이터

월별 / 경쟁사별 평균 ETA를 라인 그래프로 시각화하기 적합하다.

![image](https://user-images.githubusercontent.com/22818292/230854842-cdd15525-ad08-485b-8d1c-bea581eee93f.png)



### `elapsed`
예상 도착시간을 예측하는 ML 모델에 제공하기 위한 데이터

### `market_share`
경쟁사별 / 월별 점유율을 나타내는 데이터

월별 / 경쟁사별 라인 그래프로 시각화하기 적합하다.

![image](https://user-images.githubusercontent.com/22818292/230846096-7ea1c8c7-2412-4546-ab4a-60052d3a4a01.png)

### `popular_location`
택시 수요가 많은 인기 지역 및 급상승 인기 지역 데이터
