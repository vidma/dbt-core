import json
import os
import pytest

from test.integration_rework.base import DBTIntegrationTest


class BaseTestSimpleCopy(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_copy_001"

    @staticmethod
    def dir(path):
        return path.lstrip('/')

    @property
    def models(self):
        return self.dir("models")
    
    @property
    def project_config(self):
        return self.seed_quote_cfg_with({
            'profile': '{{ "tes" ~ "t" }}'  # why is this string split?
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
    

class TestSimpleCopy(BaseTestSimpleCopy):

    def seed_files(self, data):
        with open("seeds/seed.csv", "w") as f:
            f.write(data)
    
    def models(self, data):
        for k, v in data.items():
            with open(f"models/{k}.sql", "w") as f:
                f.write(v)

    def seed_data(self):
        seed = {}
        seed['initial'] = """id,first_name,last_name,email,gender,ip_address
        1,Jack,Hunter,jhunter0@pbs.org,Male,59.80.20.168
        2,Kathryn,Walker,kwalker1@ezinearticles.com,Female,194.121.179.35
        3,Gerald,Ryan,gryan2@com.com,Male,11.3.212.243
        """
        seed['updated']  = """id,first_name,last_name,email,gender,ip_address
        1,Jack,Hunter,jhunter0@pbs.org,Male,59.80.20.168
        2,Kathryn,Walker,kwalker1@ezinearticles.com,Female,194.121.179.35
        3,Gerald,Ryan,gryan2@com.com,Male,11.3.212.243
        4,Bonnie,Spencer,bspencer3@ameblo.jp,Female,216.32.196.175
        5,Harold,Taylor,htaylor4@people.com.cn,Male,253.10.246.136
        """
        return seed
    
    def model_data(self):
        model = {}

        model['view_model'] = """
            {{
            config(
                materialized = "view"
            )
            }}
            select * from {{ ref('seed') }}
            """
    
        model['incremental'] = """
            {{
            config(
                materialized = "incremental"
            )
            }}
            select * from {{ ref('seed') }}
            {% if is_incremental() %}
                where id > (select max(id) from {{this}})
            {% endif %}
            """
        
        model['materialized'] = """
            {{
            config(
                materialized = "table"
            )
            }}
            -- ensure that dbt_utils' relation check will work
            {% set relation = ref('seed') %}
            {%- if not (relation is mapping and relation.get('metadata', {}).get('type', '').endswith('Relation')) -%}
                {%- do exceptions.raise_compiler_error("Macro " ~ macro ~ " expected a Relation but received the value: " ~ relation) -%}
            {%- endif -%}
            -- this is a unicode character: å
            select * from {{ relation }}
            """
        
        model['get_and_ref'] = """
            {%- do adapter.get_relation(database=target.database, schema=target.schema, identifier='materialized') -%}

            select * from {{ ref('materialized') }}
            """
        
        model['interleavened_sort'] = """
        {{
        config(
            materialized = "table",
            sort = ['first_name', 'last_name'],
            sort_type = 'interleaved'
        )
        }}
        select * from {{ ref('seed') }}
        """

        model['compound_sort'] = """
        {{
        config(
            materialized = "table",
            sort = 'first_name',
            sort_type = 'compound'
        )
        }}

        select * from {{ ref('seed') }}
        """

        model['advanced_incremental'] = """
        {{
            config(
                materialized = "incremental",
                unique_key = "id",
                persist_docs = {"relation": true}
            )
        }}

        select * from {{ ref('seed') }}

        {% if is_incremental() %}
            where id > (select max(id) from {{this}})
        {% endif %}
        """

        model['disabled'] = """
        {{
        config(
            materialized = "view",
            enabled = False
        )
        }}

        select * from {{ ref('seed') }}
        """

        model['empty'] = ""



        return model

    @pytest.mark.use_profile.with_args(profile="postgres")
    def test__simple_copy(self):
        seed_files = self.seed_data()
        self.models(self.model_data())

        self.seed_files(seed_files['initial'])
        results = self.run_dbt(["seed"])
        # breakpoint()
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized", "get_and_ref"])
        
        self.seed_files(seed_files['updated'])
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results), 1)
        results = self.run_dbt()
        self.assertEqual(len(results), 7)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized", "get_and_ref"])

    # @use_profile('postgres')
    # def test__postgres__simple_copy_with_materialized_views(self):
    #     self.run_sql('''
    #         create table {schema}.unrelated_table (id int)
    #     '''.format(schema=self.unique_schema())
    #     )
    #     self.run_sql('''
    #         create materialized view {schema}.unrelated_materialized_view as (
    #             select * from {schema}.unrelated_table
    #         )
    #     '''.format(schema=self.unique_schema()))
    #     self.run_sql('''
    #         create view {schema}.unrelated_view as (
    #             select * from {schema}.unrelated_materialized_view
    #         )
    #     '''.format(schema=self.unique_schema()))
    #     results = self.run_dbt(["seed"])
    #     self.assertEqual(len(results),  1)
    #     results = self.run_dbt()
    #     self.assertEqual(len(results),  7)

    # @use_profile("postgres")
    # def test__postgres__dbt_doesnt_run_empty_models(self):
    #     results = self.run_dbt(["seed"])
    #     self.assertEqual(len(results),  1)
    #     results = self.run_dbt()
    #     self.assertEqual(len(results),  7)

    #     models = self.get_models_in_schema()

    #     self.assertFalse("empty" in models.keys())
    #     self.assertFalse("disabled" in models.keys())


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
