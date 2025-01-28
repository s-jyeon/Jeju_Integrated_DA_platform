<<<<<<< HEAD
# ip-jeju-data-platform-airflow

### SeSac 강동 | 클라우드 데이터 엔지니어 과정 | 팀 IP 파이널 프로젝트

---

# Airflow - Local

### 세팅 개요

##### 가상 환경 세팅 및 활성화
```shell
python -m venv .venv

source ./.venv/bin/activate  # macOS
./venv/Scripts/activate.bat  # Windows

# 비활성화
deactivate 
```
> airflow 사용이 완료되면 반드시 비활성화 할 것!

##### 패키지 설치
```shell
pip install -r requirements.txt
```

##### AIRFLOW_HOME 환경 변수의 값을 프로젝트 디렉터리로 세팅
```shell
export AIRFLOW_HOME=$PWD
```

<!-- ##### Airflow 관련 디렉터리 생성
```shell
mkdir -p ./dags ./logs ./plugins ./config
``` -->

<!-- ##### Airflow 설정 파일(`airflow.cfg` 생성)
```shell
airflow config list --default > ./airflow.cfg
``` -->

##### Airflow - MetaDB 생성
```shell
airflow db init
```

##### Airflow - User 생성
```shell
airflow users create \
		--username admin \
		--password admin \
		--firstname ip \
		--lastname sesac \
		--email test@test.com \
		--role Admin
```

##### Airflow 실행
> 터미널 2개 필요

```shell
airflow webserver --port 8080
airflow scheduler
```

##### Airflow UI 세팅

=======
# Jeju_Integrated_DA_platform
>>>>>>> 14aa962be48a028db810fa540a905145520294ef
