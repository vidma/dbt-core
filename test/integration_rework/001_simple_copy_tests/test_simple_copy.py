import json
import os
import pytest

from test.integration_rework.base import DBTIntegrationTest


class BaseTestSimpleCopy(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_copy_001"
    
    @property
    def project_config(self):
        return self.seed_quote_cfg_with({
            'profile': '{{ "tes" ~ "t" }}'  # why is this string split???
        })

    def seed_quote_cfg_with(self, extra):
        cfg = {
            'config-version': 2,
            'seeds': {
                'quote_columns': False,  #default is True, but why do we want this to be False?
            }
        }
        cfg.update(extra)
        return cfg
    

# class TestShouting(BaseTestSimpleCopy):
#     @property
#     def models(self):
#         return self.dir('models-shouting')

#     @property
#     def project_config(self):
#         return self.seed_quote_cfg_with({"seed-paths": [self.dir("seed-initial")]})

#     @use_profile("postgres")
#     def test__postgres__simple_copy_loud(self):
#         results = self.run_dbt(["seed"])
#         self.assertEqual(len(results),  1)
#         results = self.run_dbt()
#         self.assertEqual(len(results),  7)

#         self.assertManyTablesEqual(["seed", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])

#         self.use_default_project({"seed-paths": [self.dir("seed-update")]})
#         results = self.run_dbt(["seed"])
#         self.assertEqual(len(results),  1)
#         results = self.run_dbt()
#         self.assertEqual(len(results),  7)

#         self.assertManyTablesEqual(["seed", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"])


# # I give up on getting this working for Windows.
# @mark.skipif(os.name == 'nt', reason='mixed-case postgres database tests are not supported on Windows')
# class TestMixedCaseDatabase(BaseTestSimpleCopy):
#     @property
#     def models(self):
#         return self.dir('models-trivial')

#     def postgres_profile(self):
#         return {
#             'config': {
#                 'send_anonymous_usage_stats': False
#             },
#             'test': {
#                 'outputs': {
#                     'default2': {
#                         'type': 'postgres',
#                         'threads': 4,
#                         'host': self.database_host,
#                         'port': 5432,
#                         'user': 'root',
#                         'pass': 'password',
#                         'dbname': 'dbtMixedCase',
#                         'schema': self.unique_schema()
#                     },
#                 },
#                 'target': 'default2'
#             }
#         }

#     @property
#     def project_config(self):
#         return {'config-version': 2}

#     @use_profile('postgres')
#     def test_postgres_run_mixed_case(self):
#         self.run_dbt()
#         self.run_dbt()


# class TestQuotedDatabase(BaseTestSimpleCopy):

#     @property
#     def project_config(self):
#         return self.seed_quote_cfg_with({
#             'quoting': {
#                 'database': True,
#             },
#             "seed-paths": [self.dir("seed-initial")],
#         })

#     def seed_get_json(self, expect_pass=True):
#         results, output = self.run_dbt_and_capture(
#             ['--debug', '--log-format=json', '--single-threaded', 'seed'],
#             expect_pass=expect_pass
#         )

#         logs = []
#         for line in output.split('\n'):
#             try:
#                 log = json.loads(line)
#             except ValueError:
#                 continue

#             # TODO structured logging does not put out run_state yet
#             # if log['extra'].get('run_state') != 'internal':
#             #     continue
#             logs.append(log)

#         # empty lists evaluate as False
#         self.assertTrue(logs)
#         return logs

#     @use_profile('postgres')
#     def test_postgres_no_create_schemas(self):
#         logs = self.seed_get_json()
#         for log in logs:
#             msg = log['msg']
#             self.assertFalse(
#                 'create schema if not exists' in msg,
#                 f'did not expect schema creation: {msg}'
#             )
