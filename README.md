<br/>

# 프로젝트 개요

해당 프로젝트는 분석적 관점에서 특정 데이터로 얻어낼 수 있을 인사이트를 제공하기도 하지만, 데이터 엔지니어링 분야에서 주로 쓰이는 서비스(AWS, Airflow, Spark, ...)를 활용하여 데이터를 처리하는 엔지니어링 프로세스를 구현하는데 초점을 둔다.
<br/>
<br/>
<br/>





# 시나리오

> **TLC Taxi Record Data**
>
> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page <br/>
> TLC는 뉴욕의 택시와 모빌리티를 관리하는 기관이다.<br/>
> TLC는 2009년부터 Trip Record 데이터를 취합했으며, 매년 수십 GB에 달하는 방대한 데이터를 무료로 제공한다.<br/>
> 포함된 정보는 승하차 시간/위치, 이동거리, 승객 수, 요금 등의 정보이다.
<br/>
 
TLC Taxi Record 데이터셋 중 2019년부터 제공되기 시작한 우버(Uber), 리프트(Lyft) 같은 차량공유서비스의 Trip Record 데이터인 **High Volume For-Hire Vehicle Trip Records (HVFHV)** 데이터셋을 활용하여 다음과 같은 분석 데이터를 생성하는 데이터 엔지니어링을 하고자 한다.
<br/>
<br/>

### ML 학습용 데이터 서빙

ETA(예상도착시간)를 예측하는 ML 학습용 데이터를 생성한다.

> **기대 효과**
>
> 서비스 사용자에게 ETA(예상도착시간) 정보를 제공한다.

<br/>

### 서비스 품질 분석

경쟁사와 자사의 콜 요청장소까지 택시가 도착하는 소요시간을 비교 분석하는 데이터를 생성한다.

> **기대 효과**
>
> 택시가 콜 요청장소까지 도착하는 경로 최적화같은 서비스 품질을 개선하는 액션을 취할 수 있다.<br/> 
> 또한 소요시간이 매출에 영향을 미치는지 매출과의 상관관계 등도 추가 분석할 수 있다.

<br/>

### 시장 점유율 수요 분석
경쟁사와 자사의 서비스를 얼마나 사용하는지 비교 분석하는 데이터를 생성한다.

> **기대 효과**
>
> 시장 규모를 파악하고, 시장에서 자사의 서비스가 어느 정도 수요가 있는지 파악한다.<br/>
> 이 추이를 보고 이용자 감소 원인 파악, 점유율 증대를 위한 전략 등을 고민할 수 있다.

<br/>

### 인기 지역 수요 분석

택시 수요가 많은 인기 지역을 분석하는 데이터를 생성한다.

더 나아가 인기 급상승 지역을 분석하여 떠오르는 지역이 어디인지 파악한다.

> **기대 효과**
>
> 지역 수요를 예측하고, 배차 할당을 조절할 수 있다.

<br/>
<br/>

이런 분석 결과는 비즈니스 의사결정을 지원하거나 바로 서비스에 적용된다. 그렇기 때문에 일회성으로 분석되기보다는 지속적으로 분석되어져야 한다.

즉 분석 결과 데이터(Metric)가 지속적으로 누적되며 시간 경과에 따른 트렌드(추이)를 만들고, 이를 모니터링함으로서 비즈니스 전략을 수립하거나 서비스를 고도화시킨다.

데이터 엔지니어링 측면에서의 과제는 이런 가공된 데이터를 지속적으로 서빙하는 것이다. 

따라서 해당 프로젝트는 일회성이 아닌 자동화를 고려한 설계를 바탕으로, Production 환경에서 사용할 정도 수준으로 구현하였다.

또한 고가용성(HA), 내결함성(FT), 탄력성(Elastic) 등의 엔지니어링 전략에 대해 깊게 다루진 않지만, 이런 특성을 잘 갖추어 표준처럼 자리잡은 서비스들을 활용하여 구현하였다.
<br/>
<br/>
<br/>





# 서비스 및 용도

사용하는 서비스와 용도는 다음과 같다.

<br/>

### AWS - RDS (MySQL)

Airflow 메타데이터 DB로 사용, 수집할 데이터셋에 대한 메타데이터 제공, 커스텀 로깅 용도

Airflow 메타 DB 외에 데이터셋에 대한 메타정보와 데이터셋 수집 시 별도로 로그를 저장하는 `tlc_taxi`라는 DB를 두었다.

![image](https://user-images.githubusercontent.com/22818292/230822983-ddcf92a2-4770-4607-a49f-d03c6e4810e3.png)

<br/>

**`dataset_meta`**

가져올 데이터셋에 대한 ID를 부여한 Master 테이블이다.

![image](https://user-images.githubusercontent.com/22818292/230822811-b91c61b0-8455-41f5-99a5-56f9091bd286.png)

<br/>

**`dataset_log`**

데이터셋 수집 시 Custom log를 저장하는 테이블이다.

![image](https://user-images.githubusercontent.com/22818292/231337475-54b18136-4324-4580-83e4-4a477801ccf1.png)

![image](https://user-images.githubusercontent.com/22818292/231337309-ee7694f8-752d-4ce5-b251-bfa88129c15e.png)

<br/>

### AWS - S3
수집한 데이터셋 저장, 가공한 데이터셋 저장 용도

아래와 같은 구조를 가진 DataLake로서 활용하며, 월별 데이터를 수집하지만, 대용량 처리를 위해 연도 단위로 파티셔닝 한다.

![image](https://user-images.githubusercontent.com/22818292/231336278-4637210b-7074-451e-a408-6b18bfd743b4.png)
 　 ![image](https://user-images.githubusercontent.com/22818292/231336340-65904244-092a-4f13-b06f-2257adb083ae.png)

<br/>

### AWS - EC2

Airflow Cluster 구성을 위해 사용

Airflow 관리형 서비스인 AWS MWAA를 쓸 수 있지만, Airflow Cluster에 대한 이해도를 높이기 위해 여러 대의 EC2로 날 것으로 직접 구축하였다.

<br/>
  
### AWS - EMR

관리형 Spark 서비스 사용 목적

Spark Cluster까지 EC2 날 것으로 구성하려면 피로도가 높다. 

EMR은 서비스 하나로 Spark, Hadoop, YARN, Jupyter Notebook 같은 다양한 Application을 간편하게 구성할 수 있도록 도와주고, Application에서 제공하는 모니터링 UI를 제공하며, S3에 로깅도 해주기 때문에 debugging도 용이하다. 

<br/>

### Airflow

데이터 수집 프로세스와 분석 프로세스에 대한 Workflow 정의 및 스케줄링에 사용

이 각 프로세스는 각 DAG와 매핑된다.

- 데이터 수집 프로세스 = 데이터 수집 DAG
- 데이터 분석 프로세스 = 데이터 가공 및 분석 DAG

<br/>

### Spark

S3에 적재된 대량의 데이터셋을 연도 단위로 분석하고 가공하는데 사용

<br/>

### GitHub

Airflow DAG, Spark script에 대한 저장소 및 배포 자동화를 위해 사용
<br/>
<br/>
<br/>




# 아키텍처

![image](https://user-images.githubusercontent.com/22818292/231339407-38097f4e-d91f-4615-b43d-20daf4c1afa3.png)

## 프로세스 개요

세부 설명은 아래에서 하고, 전체적인 큰 프로세스를 살펴보면 다음과 같다.

<br/>

### Airflow / Spark Script 개발 및 배포 프로세스

Local에서 Airflow DAG / Spark Script 개발 후 GitHub에 Push를 하게 되면, Airflow Cluster Node들에 Airflow DAG가 배포되고, S3에 Spark Script가 배포된다.

### 데이터 수집 프로세스

1. RDS (MySQL)의 데이터셋 메타정보 테이블에서 수집할 데이터셋에 대한 링크 정보를 가져온다.

2. 수집 프로레스를 거쳐 수집한 데이터셋을 S3에 저장한다.

### 데이터 분석 프로세스
1. EMR Cluster를 생성하고 EMR의 Spark Job을 실행한다.

2. 실행된 EMR Spark Application은 S3에서 Spark Script와 데이터를 읽어 분석 및 가공한다.

3. 가공된 데이터셋을 S3에 저장한다.
<br/>
<br/>
<br/>



## Cluster 아키텍처

### Airflow Cluster

Airflow Cluster는 아래와 같은 EC2 Node들로 구성된다.

무거운 작업은 EMR로 위임할 것이기 때문에 Airflow Cluster 사양이 굳이 높을 필요는 없다.

비용 및 리소스 낭비를 줄이기 위해 높은 사양은 사용하지 않는다.

<br/>

> 데이터 수집 프로세스에서는 EMR Cluster를 사용하지 않기 때문에 Airflow Worker Node의 리소스를 사용하는데, 
>
> Task의 Memory 사용을 최적화하여 Worker Node의 사양을 낮게 유지할 수 있도록 하였다.
>
> https://velog.io/@jskim/Airflow-Task-Memory-사용-최적화하기

<br/>

![image](https://user-images.githubusercontent.com/22818292/230561284-e3cd3750-e8fa-4b41-ae2f-7d021cc8c7aa.png)

![image](https://user-images.githubusercontent.com/22818292/230561536-4a0e6804-dcb2-4a94-ae42-a139ef0cf6d7.png)

<br/>
<br/>

### Airflow Cluster 컴포넌트 및 프로세스

Airflow Cluster를 이루는 컴포넌트들을 좀 더 자세히 살펴보면 다음과 같다.

> **참고**
> 
> 내용 편의를 위해 현재 섹션에서 **Airflow DAG / Spark Script 개발 및 배포 프로세스**에 대한 아키텍처도 함께 설명한다.

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

<br/>

**airflow-worker\*** : 할당된 Task를 실행한다.

<br/>

**GitHub Actions Runner** : `CeleryExecutor` 사용 시, Celery Worker가 DAG 폴더에 접근할 수 있어야 한다. 그리고 Node들이 동기화 된 DAG를 봐라봐야 한다.

예를 들어 

Primary Node가 바라보는 DAG가 최신화 되어 있고, Worker Node가 바라보는 DAG는 최신화가 안 되어 있다면 Web UI에서는 최신화 된 DAG Logic을 볼 수 있지만, Task 실행 시 최신화 된 Logic을 실행하지 못한다.

따라서 DAG 개발 및 배포의 편의성 측면과 DAG Sync 측면에서 GitHub 저장소와 GitHub Actions를 사용한다. 

Push가 일어났을 때 Airflow Cluster의 모든 Node들의 DAG 폴더를 자동으로 Update 및 Sync 할 수 있도록 하기 위해서 각 Node에 GitHub Actions Self-hosted Runner를 설치하고 구성한다.

<br/>

**Statsd Exporter** : Airflow Metric을 제공하는 프로세스이다. 이후에 Prometheus와 Grafana와 연동할 예정이라서 미리 구성해두었다.

<br/>

**RDS** : RDS (MySQL)의 `airflow` DB를 Airflow 메타데이터를 저장하는 DB로 사용한다.

![image](https://user-images.githubusercontent.com/22818292/229800380-274fff08-cf35-470c-9dab-36d25c66d86a.png)

<br/>
<br/>
<br/>

### EMR Cluster

아래에 정의한 `JOB_FLOW_OVERRIDES` 정의를 이용하여 Spark만 사용할 예정이다.

수집되는 월별 데이터를 Spark가 연도 파티션 단위로 처리할 예정이기 때문에, 연도 파티션에 데이터가 누적될수록 Spark Cluster Node 리소스도 꽤 필요하다.

따라서 Node 사양은 `m5.xlarge` 유형을 사용하고, 2대의 Core Node로 구성한다.

![image](https://user-images.githubusercontent.com/22818292/230817404-a314a541-4598-4c96-981e-06fc7b06fa8c.png)

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
    ],
    "StepConcurrencyLevel": 3
}
```
<br/>
<br/>
<br/>









# 파일 구조

```
airflow-spark-emr
├── .github
│   └── workflows
│       └── deploy.yaml
├── airflow_dags
│   ├── analyze_tlc_taxi_record.py
│   └── download_tlc_taxi_record.py
└── spark_scripts
    ├── analyze_elapsed_time.py
    ├── analyze_market_share.py
    ├── analyze_popular_location.py
    ├── prepare_eta_prediction.py
    └── preprocess_data.py
```

- `deploy.yaml` : `airflow_dags` 폴더에 있는 Airflow DAGs를 Airflow Cluster에, `spark_scripts` 폴더에 있는 Spark Scripts를 S3에 Deploy하는 프로세스를 정의한 문서

- `airflow_dags` : Airflow DAGs를 담은 폴더

  - `analyze_tlc_taxi_record.py` :  EMR Cluster를 생성하고, 데이터셋을 분석 및 가공하는 Spark Job을 실행하는 DAG
  
  - `download_tlc_taxi_Record.py` : TLC Taxi Record 데이터를 수집해서 S3에 저장하는 DAG
 
- `spark_scripts` : Spark에서 실행할 Scripts를 담은 폴더

  - `analyze_elapsed_time.py` : 소요시간에 대한 분석 데이터를 생성하는 Script

  - `analyze_market_share.py` : 시장 점유율 분석 데이터를 생성하는 Script

  - `analyze_popular_location.py` : 인기 지역 분석 데이터를 생성하는 Script
  
  - `prepare_eta_prediction.py` : ETA(도착예정시간) 예측을 위한 학습용 데이터를 생성하는 Script
  
  - `preprocess_data.py` : 본격적으로 분석하기 전에 데이터를 전처리하는 Script
  
<br/>
<br/>
<br/>






# 프로세스 세부 설명

메인 프로세스는 두 가지로 구분된다.

- 데이터 수집 프로세스 (`download_tlc_taxi_record` DAG)
- 데이터 분석 프로세스 (`analyze_tlc_taxi_record` DAG)

![image](https://user-images.githubusercontent.com/22818292/231347998-896f35be-b58a-4080-a507-2888732d307b.png)

<br/>
<br/>

## 데이터 수집 프로세스

### 수집 로직

**download_tlc_taxi_record.py**

![image](https://user-images.githubusercontent.com/22818292/231070462-c8d506f0-9431-4478-875b-289a044d5826.png)

```python
) as dag:

    get_latest_dataset_id = get_latest_dataset_id()
    get_urls = get_url(num=2)

    get_latest_dataset_id >> get_urls

    fetch = fetch.expand(url=get_urls)

    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_analyze_tlc_taxi_record_dag",
        trigger_dag_id="analyze_tlc_taxi_record"
    )

    fetch >> trigger_dag
```

<br/>

**get_latest_dataset_id**

`dataset_log` 로그 테이블에서 마지막으로 처리된 데이터셋 ID를 가져온다.

<br/>

**get_url**

`dataset_meta` 메타 테이블에서 이번 실행에 수집할 데이터셋의 링크를 가져온다.

이번 실행에 수집할 데이터셋 링크는 `get_latest_dataset_id`에서 가져온 데이터셋 ID 이후의 ID를 기준으로 `num` 파라미터에 설정된 수만큼의 데이터셋 링크를 가져온다.

<br/>

**fetch [n]**

Dynamic Task Mapping 개념을 이용해서 데이터를 수집한다.

Dynamic Task Mapping은 Runtime 때 `n`개의 Task를 생성한다.

데이터셋에 대한 수집 프로세스를 병렬로 처리하기 위하여 사용하였으며, `get_url`의 `num` 파라미터 수만큼의 데이터셋을 병렬로 수집하게 된다.

<br/>

**trigger_analyze_tlc_taxi_record_dag**

데이터 수집 프로세스가 끝나면 데이터 분석 프로세스 (`analyze_tlc_taxi_record`) DAG를 Trigger한다.

---

수집 프로세스의 이번에 수집할 데이터셋 정보와 병렬 처리 로직을 좀 더 설명하면 다음과 같다.

예를들어

`2019-03` 데이터셋까지 수집된 상태이고, 현재 설정이 

- `num=2`이면, `2019-04`, `2019-05` 데이터
- `num=3`이면,  `2019-04`, `2019-05`, `2019-06` 데이터

가 현재 실행에서 **병렬**로 수집된다.

`num=2`
![image](https://user-images.githubusercontent.com/22818292/230826943-c962530f-3939-46bf-8cf7-87e075bf545e.png)
 `num=3`
![image](https://user-images.githubusercontent.com/22818292/230828233-5b87564e-650d-4e40-9f16-77bf1c83aa2a.png)

<br/>

### 수집 데이터
수집 프로세스에 의해 수집된 데이터는 S3의 `source` 폴더에 연도 파티션 단위로 저장된다.

데이터셋은 `parquet` 포맷으로, 하나당 보통 `500MB`는 되고, 연도당 `6GB`가 넘는다.<br/>

> parquet는 압축률이 좋기 때문에, CSV로 치면 최소 수 십 `GB`는 된다.

![image](https://user-images.githubusercontent.com/22818292/230821705-2ae3a083-e6a5-4953-9dbd-35e358113f94.png)

<br/>

## 데이터 분석 프로세스

데이터 분석 프로세스에서는 Airflow Pool & Slot과 EMR StepConcurrencyLevel을 이용한 병렬 처리 케이스를 포함한다.

Task 레벨에서의 병렬 처리를 다룰 예정이지만, 

병렬 처리 Logic을 DAG 레벨로 분리한 뒤, 동일한 논리를 적용하면 DAG 레벨에서의 병렬 처리도 별반 다르지 않다.

<br/>

### 분석 로직

**analyze_tlc_taxi_record.py**

![image](https://user-images.githubusercontent.com/22818292/231532178-1d487d7c-5cd9-428f-a4af-476d2d03b4db.png)

```python
) as dag:

    get_latest_year_partition = PythonOperator(
        task_id="get_latest_year_partition",
        python_callable=get_latest_year_partition
    )

    create_job_flow = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    with TaskGroup('preprocess', tooltip="Task for Preprocess Data") as preprocess:
        make_preprocess_data_definition = PythonOperator(
            task_id="make_preprocess_data_definition",
            python_callable=make_preprocess_data_definition
        )

        preprocess_data = EmrAddStepsOperator(
            task_id="preprocess_data",
            job_flow_id=create_job_flow.output,
            steps=make_preprocess_data_definition.output,
            wait_for_completion=True
        )

    with TaskGroup('prepare_eta_prediction', tooltip="Task for ETA Prediction") as prepare_prediction:
        make_prepare_eta_prediction_definition = PythonOperator(
            task_id="make_prepare_eta_prediction_definition",
            python_callable=make_prepare_eta_prediction_definition
        )

        prepare_eta_prediction = EmrAddStepsOperator(
            task_id="prepare_eta_prediction",
            job_flow_id=create_job_flow.output,
            steps=make_prepare_eta_prediction_definition.output,
            wait_for_completion=True,
            pool='preprocess_pool',
            priority_weight=3
        )

    with TaskGroup('analyze_1', tooltip="Task for Elapsed Time") as analyze_1:
        make_analyze_elapsed_time_definition = PythonOperator(
            task_id="make_analyze_elapsed_time_definition",
            python_callable=make_analyze_elapsed_time_definition
        )

        analyze_elapsed_time = EmrAddStepsOperator(
            task_id="analyze_elapsed_time",
            job_flow_id=create_job_flow.output,
            steps=make_analyze_elapsed_time_definition.output,
            wait_for_completion=True,
            pool='preprocess_pool',
            priority_weight=3
        )

    with TaskGroup('analyze_2', tooltip="Task for Market Share") as analyze_2:
        make_analyze_market_share_definition = PythonOperator(
            task_id="make_analyze_market_share_definition",
            python_callable=make_analyze_market_share_definition
        )

        analyze_market_share = EmrAddStepsOperator(
            task_id="analyze_market_share",
            job_flow_id=create_job_flow.output,
            steps=make_analyze_market_share_definition.output,
            wait_for_completion=True,
            pool='preprocess_pool',
            priority_weight=2
        )

    with TaskGroup('analyze_3', tooltip="Task for Popular Location") as analyze_3:
        make_analyze_popular_location_definition = PythonOperator(
            task_id="make_analyze_popular_location_definition",
            python_callable=make_analyze_popular_location_definition
        )

        analyze_popular_location = EmrAddStepsOperator(
            task_id="analyze_popular_location",
            job_flow_id=create_job_flow.output,
            steps=make_analyze_popular_location_definition.output,
            wait_for_completion=True,
            pool='preprocess_pool',
            priority_weight=1
        )

    check_job_flow = EmrJobFlowSensor(
        task_id="check_job_flow",
        job_flow_id=create_job_flow.output,
        target_states='WAITING'
    )

    remove_cluster = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id=create_job_flow.output
    )

chain(
    get_latest_year_partition,
    create_job_flow,
    preprocess,
    [prepare_prediction, analyze_1, analyze_2, analyze_3],
    check_job_flow,
    remove_cluster
)
```

<br/>

**get_latest_year_partition**

S3에 수집된 데이터를 파티션하는 연도 파티션에서 마지막 연도 파티션 정보를 가져온다. 

이는 연도 파티션 단위로 Spark Job이 실행되고, 스케줄에 의해 실행될 때마다 마지막 연도의 데이터를 가져와야 자동화가 되기 때문이다.

예를 들어

`2019-11` 데이터가 수집될 때 `2019` 파티션에 저장되고, Spark는 2019년 데이터에 대해 처리한다.

다음 스케줄에는 `2019-12` 데이터가 수집되며 마찬가지로 `2019` 파티션에 저장되고, Spark는 2019년 데이터에 대해 처리한다.

> 실행 시마다 해당연도 파티션에 월별 데이터가 누적되고, Spark가 처리하는 데이터양도 누적된다.

<br/>

다음 스케줄에는 `2020-01` 데이터가 수집되며 `2020` 파티션에 저장된다. Spark는 2020년 데이터에 대해 처리한다.

이 Flow를 자동화 하기 위해서는 마지막 연도 정보를 가져오는 동적 처리가 필요하다.

<br/>

**create_job_flow**

[EMR Cluster 아키텍처](#user-content-emr-cluster) 섹션에서 정의한 `JOB_FLOW_OVERRIDES` 정의를 기반으로 EMR Cluster를 생성한다.

<br/>

**preprocess**

- make_preprocess_data_definition

  `preprocess_data` Task 실행을 위한 동적 Spark Submit 정의를 생성한다. 
  
  ```yaml
  STEP = [
        {
            "Name": "Preprocess TLC Taxi Record",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    f"s3://{bucket}/{script}/preprocess_data.py",
                    "--src",
                    f"s3://{bucket}/{src}/{latest_year}/",
                    "--output",
                    f"s3://{bucket}/{output}/preprocess/{latest_year}/",
                ]
            }
        }
    ]
  ```
  
- preprocess_data
  
  전처리 데이터를 생성하는 Spark Job을 실행한다. 
  
<br/>

---
이후에는 전처리된 데이터를 기반으로 4개의 분석 데이터를 생성한다.

이 데이터들은 전처리된 데이터를 공통으로 사용하기 때문에 병렬 처리로 진행한다. 

<br/>

**prepare_prediction**

- make_prepare_eta_prediction_definition

  `prepare_eta_prediction` Task 실행을 위한 동적 Spark Submit 정의를 생성한다.
  
  ```yaml
      STEP = [
        {
            "Name": "Prepare Data for ETA Prediction",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    f"s3://{bucket}/{script}/prepare_eta_prediction.py",
                    "--src",
                    f"s3://{bucket}/{output}/preprocess/{latest_year}/",
                    "--output",
                    f"s3://{bucket}/{output}/analyze/{latest_year}/",
                ]
            }
        }
    ]
  ```

- prepare_eta_prediction

  ML 학습용 데이터(예정도착시간(ETA) 예측을 위해 가공된 데이터)를 생성하는 Spark Job을 실행한다.
  
<br/>

**analyze_1**

- make_analyze_elapsed_time_definition

  `analyze_elapsed_time` Task 실행을 위한 동적 Spark Submit 정의를 생성한다. 
  
  ```yaml
  STEP = [
        {
            "Name": "Analyze Elapsed Time",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    f"s3://{bucket}/{script}/analyze_elapsed_time.py",
                    "--src",
                    f"s3://{bucket}/{output}/preprocess/{latest_year}/",
                    "--output",
                    f"s3://{bucket}/{output}/analyze/{latest_year}/",
                ]
            }
        }
    ]
  ```
  
- analyze_elapsed_time

  택시의 콜 요청장소 도착소요시간에 대한 분석 데이터(경쟁사별, 월별 평균 도착소요시간 데이터)를 생성하는 Spark Job을 실행한다.

<br/>

**analyze_2**

- make_analyze_market_share_definition

  `analyze_market_share` Task 실행을 위한 동적 Spark Submit 정의를 생성한다. 
  
  ```yaml
  STEP = [
        {
            "Name": "Analyze Market Share",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    f"s3://{bucket}/{script}/analyze_market_share.py",
                    "--src",
                    f"s3://{bucket}/{output}/preprocess/{latest_year}/",
                    "--output",
                    f"s3://{bucket}/{output}/analyze/{latest_year}/",
                ]
            }
        }
    ]
  ```
  
- analyze_market_share

  시장 점유율 분석 데이터(경쟁사별, 월별 점유율 데이터)를 생성하는 Spark Job을 실행한다.

<br/>

**analyze_3**

- make_analyze_popular_location_definition

  `analyze_popular_location` Task 실행을 위한 동적 Spark Submit 정의를 생성한다. 
  
  ```yaml
  STEP = [
        {
            "Name": "Analyze Popular Location",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    f"s3://{bucket}/{script}/analyze_popular_location.py",
                    "--src",
                    f"s3://{bucket}/{output}/preprocess/{latest_year}/",
                    "--output",
                    f"s3://{bucket}/{output}/analyze/{latest_year}/",
                ]
            }
        }
    ]
  ```
  
- analyze_popular_location

  인기 지역 분석 데이터(월별 인기 지역, 인기 급상승 지역 데이터)를 생성하는 Spark Job을 실행한다.

<br/>

**check_job_flow**

EMR Cluster가 Job들을 수행 후 유휴 상태인지 (`WAITING` 상태) 확인한다.

<br/>

**remove_cluster**

EMR Cluster를 종료한다.

<br/>
<br/>

## 병렬처리 로직

![image](https://user-images.githubusercontent.com/22818292/231542954-30995512-132b-4e1e-a3d6-fa0a73b2bb90.png)

`preprocess_data` Task 실행 이후 4개의 Task가 병렬로 처리되는데, 이 때 4개의 Task가 동시에 실행되지는 않는다.

Airflow는 Task가 동시에 많이 실행되면 리소스 부족으로 Airflow Cluster나 다른 Task 실행에 영향을 미칠 수 있기 때문에 동시에 실행되는 Task 수에 제한을 줄 수가 있다.

> 현재 구성은 Task들이 EMR Step을 Trigger하는 용도라 Airflow Cluster의 Resource 문제는 없다.

하지만 아래와 같은 케이스가 있을 수 있으므로, 병렬처리 케이스를 다뤄보기 위해 논리를 구현하였다.

<br/>
<br/>

병렬로 구성된 4개의 Airflow Task에 의해 Trigger되는 Spark Job들은 모두 전처리된 데이터 경로인 S3의 `output/preprocess/{연도 파티션}` 경로에서 데이터를 읽는다.

![image](https://user-images.githubusercontent.com/22818292/231532969-e4c66107-dae3-475f-a3f4-f455ab6e1c53.png)

이 4개의 Spark Job이 동시에 동일 경로 데이터를 읽기 때문에, Connection을 분산하기 위해 최대 2개의 Spark Job만 동시에 읽을 수 있도록 구현하였다.

<br/>
<br/>

현재 Airflow는 2개의 Slot을 가진 `preprocess_pool` Pool이 구성되어 있다.

![image](https://user-images.githubusercontent.com/22818292/231529130-c8058d87-2210-4353-bba1-2d467d57fe3e.png)

Spark Job을 실행하는 `prepare_eta_prediction`, `analyze_elapsed_time`, `analyze_market_share`, `analyze_popular_location` Task는 이 `preprocess_pool` Pool의 Slot에서 실행된다.

단, `preprocess_pool` Pool이 2개의 Slot을 가지고 있기 때문에 최대 2개의 Task가 동시 실행이 가능하다.

따라서 4개 Task가 2개짜리 Slot을 사용할 때의 실행 우선 순위가 필요하고, 우선 순위는 다음과 같다.

<br/>

[ `prepare_eta_prediction`, `analyze_elapsed_time` ] >> `analyze_market_share` >> `analyze_popular_location`

[ 3, 3 ] >> 2 >> 1

<br/>

`prepare_eta_prediction`, `analyze_elapsed_time` Task가 동시에 실행되어서 하나가 완료되어 빈 Slot이 발생하면 

`analyze_market_share` Task가 실행되고

또 빈 Slot이 발생하면 `analyze_popular_location` Task가 실행된다.

<br/>

이 프로젝트의 경우,

`prepare_eta_prediction` Task의 Spark Job이 처리하는 데이터양이 많아 한 개의 Slot을 오랫동안 차지하고 실행하며, 

나머지 한 Slot으로 나머지 3개 Task가 `analyze_elapsed_time` >> `analyze_market_share` >> `analyze_popular_location` 순서로 실행된다.


<br/>
<br/>

![image](https://user-images.githubusercontent.com/22818292/231545674-611ef336-bcb6-44b2-a7ee-5730b0e13bec.png)

이 때, EMR Cluster의 `StepConcurrencyLevel`이 `3` 이기 때문에, Airflow Task (Spark Job) 2개가 동시 실행이 가능하다.

`StepConcurrencyLevel`이 Airflow Task (Spark Job) 동시 실행 가능 갯수보다 작으면, EMR Step이 `Pending`되면서 결국 순차 실행된다.

따라서, Airflow의 병렬 처리와 EMR Cluster의 병렬 처리 조건을 잘 고려해서 설계해야 한다.

<br/>
<br/>

## 최종 결과
최종 분석 데이터는 `output/anlayze/{연도 파티션}` 경로에 저장된다.

![image](https://user-images.githubusercontent.com/22818292/231661667-ea2d94bb-8eb5-4adb-859e-0d6b4035e084.png)

<br/>

### avg_elpased_by_month

경쟁사와 자사의 택시의 콜 요청장소 도착소요시간을 비교 분석하는 데이터

![image](https://user-images.githubusercontent.com/22818292/231222245-793006b0-fe96-459a-a587-21d43fb412f8.png)

<br/>

이 데이터는 월별 / 경쟁사별 평균 도착소요시간을 라인 그래프로 시각화하기 적합하다.

![image](https://user-images.githubusercontent.com/22818292/231358350-0f001240-3416-487c-9a0c-633ae14a01a6.png)

Uber와 Lyft가 대표 차량공유서비스 업체로 어떻게 자리매김할 수 있었는지, 

또 둘 중에서는 Uber가 압도적인 시장 점유율을 어떻게 차지할 수 있었는지 확연히 확인할 수 있다.

서비스 품질을 평가하는 요소는 다양하겠지만, 콜 요청 후 요청장소까지 도착하는 소요시간이 두 업체는 10분 내외로 적게 소모되는 것을 확인할 수 있고,

심지어 Uber는 5분 이내의 엄청 짧은 시간이 소요되는 걸 볼 수 있다.

<br/>

### elapsed_for_eta_prediction

ETA(예상도착시간)를 예측하는 ML 모델에 제공하기 위한 데이터

![image](https://user-images.githubusercontent.com/22818292/231223480-e379704f-6397-486b-a02a-ce612227032f.png)

<br/>

### market_share

경쟁사별 / 월별 점유율을 나타내는 데이터

![image](https://user-images.githubusercontent.com/22818292/231223700-b7c34733-1855-4b56-b9dc-906c0baa783e.png)

<br/>

이 데이터도 월별 / 경쟁사별 라인 그래프로 시각화하기 적합하다.

![image](https://user-images.githubusercontent.com/22818292/231358439-352991f7-65dc-4155-a11a-cf17643b8ae4.png)

Uber의 점유율이 압도적이고, 전체적인 추세를 보니 콜 요청장소까지의 도착소요시간이 상당한 상관관계가 있는게 육안으로도 확인된다.

Uber의 운행 수가 저렇게 많은데도 불구하고 평균 도착소요시간이 5분도 안 걸린다는 거는 택시 매칭 시스템이 엄청 잘되어 있기 때문으로 판단된다.

<br/>

### popular_location

택시 수요가 많은 인기 지역 및 급상승 인기 지역 데이터

![image](https://user-images.githubusercontent.com/22818292/231352918-cd537088-0f32-49fc-bd36-8b1650620ff0.png)

<br/>

이 데이터는 랭킹 차트, 트리맵 등으로 시각화하기 적합하다.

![image](https://user-images.githubusercontent.com/22818292/231361423-0c4c975d-d362-4dcf-8113-a4ec60873b52.png)

2020년 10월에 `61` 지역은 2위인 `79` 지역에 비해 0.3% 나 점유율이 높다. 다른 랭킹 간 차이를 비교해보면 상대적으로 엄청난 차이다.

이는 `61` 지역이 압도적인 수요가 있다는 뜻으로 풀 수 있다. 

대기 중인 택시를 해당 지역 주변에서 대기하도록 하는 등의 전략을 세우면 좋을 것 같다.

해당 달에 `26` 지역은 27 순위가 증가했으며, 갑자기 수요가 늘어난 지역이라고 볼 수 있다. 

이 급작스런 수요가 어떤 원인에 의한 것인지 파악하고, 미래에도 이 수요가 증가/감소될지 분석해보면 좋을 것 같다.
