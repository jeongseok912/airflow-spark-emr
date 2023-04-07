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

## 서비스

### AWS

- **RDS (MySQL)** : 수집할 데이터셋에 대한 메타데이터 제공, 수집 시 로깅 용도<br/>
RDS는 다양한 DB 엔진을 지원하며, 인스턴스 유지 관리에 소요되는 시간을 줄여주고, 읽기 전용 복제본으로 트래픽 부하를 줄이는 등 다양한 이점이 있어 많이 사용한다.<br/>

- **S3** : 수집한 데이터셋 저장, 가공한 데이터셋 저장 용도<br/>
데이터 및 요청을 파이셔닝하는데 유용하며, DataLake로 활용을 많이 한다.

- **EC2** : Airflow Multi Node Cluster 구성을 위해 사용<br/>
  Airflow 관리형 서비스인 MWAA를 쓸 수 있지만, Airflow Cluster에 대한 이해도를 높이기 위해 여러 대의 EC2로 날 것으로 구성한다.
  
- **EMR** : Spark 사용 목적<br/>
  Spark Cluster까지 EC2 날 것으로 구성하려면 피로도가 높다. EMR은 서비스 하나로 Spark, Hadoop, YARN, Jupyter Notebook 같은 다양한 Application을 간편하게 구성할 수 있도록 도와준다.<br/> 
  또한 Application에서 제공하는 모니터링 UI를 제공하고, S3에 로깅도 해주기 때문에 debugging도 용이하다. 

### Airflow
프로젝트 시나리오의 경우 크게 용도 및 목적을 2가지 프로세스로 분리 및 정의하는데, 이 프로세스를 편하게 스케줄링 및 트리거하기 위해 사용한다.<br/>
이 2개의 프로세스는 각각의 DAG 파이프라인을 말한다.
- 데이터 수집 프로세스 = 데이터 수집 DAG
- 데이터 분석 프로세스 = 데이터 가공 및 처리 DAG

### Spark
S3에 적재된 대량의 데이터셋을 분석하고 가공하는데 사용한다.<br/>
MPP DB인 Redshift에 저장하고 시각화까지 하는 파이프라인을 만들 수 있지만, 프로젝트의 범위를 S3에 추출하는 것으로 제한한다.

### GitHub
Airflow DAG에 대한 저장소, 버전관리 및 배포를 위해 사용한다.
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
1. 데이터셋 수집 프로세스가 완료되었는지 체크 후, Airflow EMR Cluster를 생성하고 EMR의 Spark submit Step을 실행한다.
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

### EMR Cluster
다음과 같은 정의로 구현한다.<br/>
<br/>
```yaml
JOB_FLOW_OVERRIDES = {
    "Name": "PySpark Cluster",
    "LogUri": "s3://airflow--log/emr-log/",
    "ReleaseLabel": "emr-6.10.0",
    "Applications": [{"Name": "Spark"}, {"Name": "JupyterEnterpriseGateway"}],
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
    "ServiceRole": "EMR_DefaultRole"
}
```

