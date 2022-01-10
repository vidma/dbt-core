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
    

class TestSimpleCopy(BaseTestSimpleCopy):

    def base_setup(self):
        self.seed_data_dict = self.seed_data()
        self.models(self.model_data())

        self.seed_files(self.seed_data_dict['initial'])
    
    def seed_files(self, data):
        '''
        create a single seed file from
        '''
        with open("seeds/seed.csv", "w") as f:
            f.write(data)
    
    def models(self, data):
        '''
        create files from the string values of the model dict
        '''
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
        '''
        I stuck all the models into a dict just to make loading them all easy.  I'm not seeing the value in having them all though.
        '''
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
    # tests updating seed data. and ref of a seed.  don't know what this has to do with copy.
    @pytest.mark.use_profile.with_args(profile="postgres")
    def test__simple_copy(self):
        self.base_setup()
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized", "get_and_ref"])
        
        self.seed_files(self.seed_data_dict['updated'])
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results), 1)
        results = self.run_dbt()
        self.assertEqual(len(results), 7)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized", "get_and_ref"])

    # tests that there can be random other tables/views?? look at this again when it's not friday.
    @pytest.mark.use_profile.with_args(profile="postgres")
    def test__simple_copy_with_materialized_views(self):
        self.run_sql('''
            create table {schema}.unrelated_table (id int)
        '''.format(schema=self.unique_schema())
        )
        self.run_sql('''
            create materialized view {schema}.unrelated_materialized_view as (
                select * from {schema}.unrelated_table
            )
        '''.format(schema=self.unique_schema()))
        self.run_sql('''
            create view {schema}.unrelated_view as (
                select * from {schema}.unrelated_materialized_view
            )
        '''.format(schema=self.unique_schema()))
        self.base_setup()
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

    #  tests what the title says!
    @pytest.mark.use_profile.with_args(profile="postgres")
    def test__dbt_doesnt_run_empty_models(self):
        self.base_setup()

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        models = self.get_models_in_schema()

        self.assertFalse("empty" in models.keys())
        self.assertFalse("disabled" in models.keys())


class TestSimpleCopy(BaseTestSimpleCopy):

    def base_setup(self):
        self.seed_data_dict = self.seed_data()
        self.models(self.model_data())

        self.seed_files(self.seed_data_dict['initial'])
    
    def seed_files(self, data: str) -> None:
        '''
        create a single seed file from
        '''
        with open("seeds/seed.csv", "w") as f:
            f.write(data)
    
    def models(self, data: Dict[str, str]) -> None:
        '''
        create files from the string values of the model dict
        '''
        view_model = """
            {{
            config(
                materialized = "view"
            )
            }}
            select * from {{ ref('seed') }}
            """
        with open(f"models/view_model.sql", "w") as f:
            f.write(view_model)

    def seed_data(self) -> str:
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
    
    def model_data(self) -> Dict:
        '''
        I stuck all the models into a dict just to make loading them all easy.  I'm not seeing the value in having them all though.
        '''
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
    # tests updating seed data. and ref of a seed.  don't know what this has to do with copy.
    @pytest.mark.use_profile.with_args(profile="postgres")
    def test__simple_copy(self):
        self.base_setup()
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized", "get_and_ref"])
        
        self.seed_files(self.seed_data_dict['updated'])
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results), 1)
        results = self.run_dbt()
        self.assertEqual(len(results), 7)

        self.assertManyTablesEqual(["seed", "view_model", "incremental", "materialized", "get_and_ref"])

    # tests that there can be random other tables/views?? look at this again when it's not friday.
    @pytest.mark.use_profile.with_args(profile="postgres")
    def test__simple_copy_with_materialized_views(self):
        self.run_sql('''
            create table {schema}.unrelated_table (id int)
        '''.format(schema=self.unique_schema())
        )
        self.run_sql('''
            create materialized view {schema}.unrelated_materialized_view as (
                select * from {schema}.unrelated_table
            )
        '''.format(schema=self.unique_schema()))
        self.run_sql('''
            create view {schema}.unrelated_view as (
                select * from {schema}.unrelated_materialized_view
            )
        '''.format(schema=self.unique_schema()))
        self.base_setup()
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

    #  tests what the title says!
    @pytest.mark.use_profile.with_args(profile="postgres")
    def test__dbt_doesnt_run_empty_models(self):
        self.base_setup()

        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        models = self.get_models_in_schema()

        self.assertFalse("empty" in models.keys())
        self.assertFalse("disabled" in models.keys())

