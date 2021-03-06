### Install Apache Airflow
- add the following lines to `~/.bashrc` and source it
    ```
    export AIRFLOW_HOME=~/airflow
    export SLUGIFY_USES_TEXT_UNIDECODE=yes  (optional, if there is a dependency issue)
    ```
- use `pip` to install Airflow
    ```
    pip install apache-airflow --user
    ```
  (add '--user' if there is a permission issue)
- check installation
    ```
    airflow version
    ```
- change the default timezone in `airflow.cfg` (optional)
    ```
    default_timezone = est
    ```


### Run Airflow DAGs
- move the python script to `$AIRFLOW_HOME/dags/`
- execute the python script
    ```
    python example.py
    ```
- initialize databases
    ```
    airflow initdb
    ```
- launch webserver
    ```
    airflow webserver -p 8081
    ```
- open another terminal; use following command to launch scheduler
    ```
    airflow scheduler
    ```
- go to the webUI, `http://<EC2 public DNS>:8081`
- enable the dag (on the left) and then execute it on the most left button on the small menu with the __'play’__ icon
