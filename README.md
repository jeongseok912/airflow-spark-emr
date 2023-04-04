# 프로젝트 개요

해당 프로젝트는 데이터 엔지니어링 분야에서 주로 쓰이는 기술 스택과 서비스(AWS, Airflow, Spark, ...)를 활용하여 데이터를 처리하는 프로세스에 대해 이해하는데 초점을 둔다.
<br/>

# 아키텍처 및 프로세스

활용할 아키텍처는 다음과 같다.
![](https://velog.velcdn.com/images/jskim/post/a80a06bb-b55c-4664-a62b-355978be3e0c/image.png)

## 기술 스택

### AWS

- **RDS (MySQL)** : 수집할 데이터셋에 대한 메타데이터 제공, 수집에 대한 로깅 용도

- **S3** : 수집한 데이터셋 저장, 가공한 데이터 저장 용도

- **EC2** : Airflow Muti Node Cluster 구성을 위해 사용
  Airflow 관리형 서비스인 MWAA를 쓸 수 있지만, Airflow Cluster에 대한 이해도를 높이기 위해 여러 대의 EC2로 날 것으로 구성한다.

- **EMR** : Spark 사용 목적
  Spark Cluster까지 EC2 날 것으로 구성하려면 피로도가 높다. Spark on EMR로도 충분하다. 더불어 Hadoop, YARN, JupyterNotebook 같은 Application에 대한 손쉬운 구성, 모니터링 UI 및 로그 제공 등도 편하다.

### Airflow

Airflow는 크게 2가지 측면으로 사용한다.
데이터 수집 프로세스같은 경우 데이터셋을 주기적으로 다운로드하고 S3에 업로드 하기 위해 사용한다.
데이터 분석 프로세스같은 경우 데이터 수집 프로세스 이후에 Spark Cluster를 생성하고 Spark Job을 실행시키기 위한 Trigger 역할을 한다.

### Spark

S3에 적재된 대량의 데이터셋을 분석 용도에 맞춰서 다양한 형태로 가공하고, 가공된 형태의 데이터셋을 사용하기 위해 추출하여 S3에 저장하는 목적으로 사용한다. MPP DB인 Redshift에 저장하고 시각화까지 할 수 있지만, 해당 프로젝트에서는 S3에 추출하는 것으로 범위를 제한한다.

### GitHub

Airflow DAG에 대한 버전관리 및 배포를 위해 사용한다.
<br/>

## 프로세스 개요

자세한 로직에 대한 설명은 아래에서 설명하고 간략하게 프로세스가 어떻게 작동하는지 알아본다.

### DAG 개발 및 배포 프로세스

ㄱ. Local에서 DAG Script 작성 후 Git Hub에 Push를 하게 되면 Git Action을 활용하여 Airflow Cluster Node들에 DAG가 배포된다.

### 데이터 수집 프로세스

1. RDS (MySQL)에서 수집할 데이터셋에 대한 링크 정보를 가져온다.
2. S3에 데이터셋을 저장한다.

### 데이터 분석 프로세스

a. Airflow EMR Cluster를 생성하고, EMR의 spark-submit Step을 실행한다.
b. 실행된 Spark Application은 데이터 수집 프로세스에서 저장한 데이터셋을 Source로 읽어들이여 가공한다. 가공된 데이터셋을 CSV로 추출하여 S3에 저장한다.
