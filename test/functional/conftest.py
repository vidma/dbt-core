# NOTE: primer on pytest fixtures and fixture scope -- https://docs.pytest.org/en/stable/fixture.html
import os
import pytest
import random
import time
from typing import Dict, Any

import yaml


@pytest.fixture
def unique_schema() -> str:
    return "test{}{:04}".format(int(time.time()), random.randint(0, 9999))


@pytest.fixture
def profiles_root(tmpdir):
    # tmpdir docs - https://docs.pytest.org/en/6.2.x/tmpdir.html
    return tmpdir.mkdir('profile')


@pytest.fixture
def project_root(tmpdir):
    # tmpdir docs - https://docs.pytest.org/en/6.2.x/tmpdir.html
    return tmpdir.mkdir('project')


def postgres_profile_data(unique_schema):
    database_host = os.environ.get('DOCKER_TEST_DATABASE_HOST', 'localhost')

    return {
        'config': {
            'send_anonymous_usage_stats': False
        },
        'test': {
            'outputs': {
                'default': {
                    'type': 'postgres',
                    'threads': 4,
                    'host': database_host,
                    'port': 5432,
                    'user': 'root',
                    'pass': 'password',
                    'dbname': 'dbt',
                    'schema': unique_schema,
                },
                'other_schema': {
                    'type': 'postgres',
                    'threads': 4,
                    'host': database_host,
                    'port': 5432,
                    'user': 'root',
                    'pass': 'password',
                    'dbname': 'dbt',
                    'schema': unique_schema+'_alt',
                }
            },
            'target': 'default'
        }
    }


@pytest.fixture
def dbt_profile_data(unique_schema):
    return postgres_profile_data(unique_schema)


@pytest.fixture
def dbt_profile(profiles_root, dbt_profile_data) -> Dict[str, Any]:
    path = os.path.join(profiles_root, 'profiles.yml')
    with open(path, 'w') as fp:
        fp.write(yaml.safe_dump(dbt_profile_data))
    return dbt_profile_data
