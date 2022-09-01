from airflow.utils.db import provide_session
from airflow.models import XCom

@provide_session
def cleanup_xcom(dag_str, session=None):
    """Clear old xcoms."""
    session.query(XCom).filter(XCom.dag_id == dag_str).delete()


