# TODO: add type hinting to make tests easier to write (using lsp and
# autocomplete for IDEs)
import json
import os
from contextlib import contextmanager
from typing import List

import yaml

from dbt.adapters.factory import get_adapter, register_adapter
from dbt.main import handle_and_check
from dbt import flags
from dbt.config import RuntimeConfig
from dbt.logger import GLOBAL_LOGGER as logger, log_manager


class ProjectDefinition:
    def __init__(
        self,
        name='test',
        version='0.1.0',
        profile='test',
        project_data=None,
        packages=None,
        models=None,
        macros=None,
        snapshots=None,
        seeds=None,
    ):
        self.project = {
            'config-version': 2,
            'name': name,
            'version': version,
            'profile': profile,
        }
        if project_data:
            self.project.update(project_data)
        self.packages = packages
        self.models = models
        self.macros = macros
        self.snapshots = snapshots
        self.seeds = seeds

    def _write_recursive(self, path, inputs):
        for name, value in inputs.items():
            if (name.endswith('.sql') or
                    name.endswith('.csv') or
                    name.endswith('.md')):
                path.join(name).write(value)
            elif name.endswith('.yml'):
                if isinstance(value, str):
                    data = value
                else:
                    data = yaml.safe_dump(value)
                path.join(name).write(data)
            else:
                self._write_recursive(path.mkdir(name), value)

    def write_packages(self, project_dir, remove=False):
        if remove:
            project_dir.join('packages.yml').remove()
        if self.packages is not None:
            if isinstance(self.packages, str):
                data = self.packages
            else:
                data = yaml.safe_dump(self.packages)
            project_dir.join('packages.yml').write(data)

    def write_config(self, project_dir, remove=False):
        cfg = project_dir.join('dbt_project.yml')
        if remove:
            cfg.remove()
        cfg.write(yaml.safe_dump(self.project))

    def _write_values(self, project_dir, remove, name, value):
        if remove:
            project_dir.join(name).remove()

        if value is not None:
            self._write_recursive(project_dir.mkdir(name), value)

    def write_models(self, project_dir, remove=False):
        self._write_values(project_dir, remove, 'models', self.models)

    def write_macros(self, project_dir, remove=False):
        self._write_values(project_dir, remove, 'macros', self.macros)

    def write_snapshots(self, project_dir, remove=False):
        self._write_values(project_dir, remove, 'snapshots', self.snapshots)

    def write_seeds(self, project_dir, remove=False):
        self._write_values(project_dir, remove, 'seeds', self.seeds)

    def write_to(self, project_dir, remove=False):
        if remove:
            project_dir.remove()
            project_dir.mkdir()
        self.write_packages(project_dir)
        self.write_config(project_dir)
        self.write_models(project_dir)
        self.write_macros(project_dir)
        self.write_snapshots(project_dir)
        self.write_seeds(project_dir)


def assert_has_threads(results, num_threads):
    assert 'logs' in results
    c_logs = [l for l in results['logs'] if 'Concurrency' in l['message']]
    assert len(c_logs) == 1, \
        f'Got invalid number of concurrency logs ({len(c_logs)})'
    assert 'message' in c_logs[0]
    assert f'Concurrency: {num_threads} threads' in c_logs[0]['message']


def get_write_manifest(querier, dest):
    result = querier.async_wait_for_result(querier.get_manifest())
    manifest = result['manifest']

    with open(dest, 'w') as fp:
        json.dump(manifest, fp)


# ** new test helpers **
def execute(adapter, sql, connection_name='tests'):
    with adapter.connection_named(connection_name):
        conn = adapter.connections.get_thread_connection()
        with conn.handle.cursor() as cursor:
            try:
                cursor.execute(sql)
                conn.handle.commit()

            except Exception as e:
                if conn.handle and conn.handle.closed == 0:
                    conn.handle.rollback()
                print(sql)
                print(e)
                raise
            finally:
                conn.transaction_open = False


class TestArgs:
    def __init__(self, profiles_dir, which='run-operation'):
        self.which = which
        self.single_threaded = False
        self.profiles_dir = profiles_dir
        self.project_dir = None
        self.profile = None
        self.target = None


@contextmanager
def built_schema(schema, project_dir, profiles_dir):
    """ This bootstraps a connection in order to set up a schema and eventually
    clean it up
    NOTE: this cds into the project dir """
    # make our args, write our project out
    args = TestArgs(profiles_dir=profiles_dir)
    # build a config of our own
    os.chdir(project_dir)
    start = os.getcwd()
    # TODO: why do we need to cd back into the temp project
    # dir after setting up runtime config? is it just to
    # bootstrap a connection to `drop schema .. create schema`?
    try:
        # TODO: this was necessary to set the correct target directory!
        flags.PROFILES_DIR = profiles_dir
        cfg = RuntimeConfig.from_args(args)
    finally:
        os.chdir(start)
    register_adapter(cfg)
    adapter = get_adapter(cfg)
    execute(adapter, 'drop schema if exists {} cascade'.format(schema))
    execute(adapter, 'create schema {}'.format(schema))
    yield
    adapter = get_adapter(cfg)
    adapter.cleanup_connections()
    execute(adapter, 'drop schema if exists {} cascade'.format(schema))


def write_profile_data(profiles_dir, profile_data):
    path = os.path.join(profiles_dir, 'profiles.yml')
    with open(path, 'w') as fp:
        fp.write(yaml.safe_dump(profile_data))
    return profile_data


def run_dbt(args: List[str], profiles_dir: str, strict: bool = True):
    log_manager.reset_handlers()

    final_args = []

    # TODO: when would you want to flip this cli arg?
    if strict:
        final_args.append('--strict')
    if os.getenv('DBT_TEST_SINGLE_THREADED') in ('y', 'Y', '1'):
        final_args.append('--single-threaded')

    final_args.extend(args)

    final_args.extend(['--profiles-dir', profiles_dir, '--log-cache-events'])

    logger.info("Invoking dbt with {}".format(final_args))
    return handle_and_check(final_args)
