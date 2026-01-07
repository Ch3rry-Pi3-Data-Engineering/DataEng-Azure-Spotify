# Databricks notebook source
# MAGIC %md
# MAGIC ## Jinja Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Parameters

# COMMAND ----------

# ============================================================
# SQL join configuration parameters
#
# Purpose
# -------
# Define a structured configuration used to dynamically
# construct a SQL query (typically via Jinja templating).
#
# Each dictionary in the list represents:
#   - one table involved in the query
#   - the alias it will be referenced by
#   - the columns to be selected
#   - (optionally) the join condition to a previously defined table
#
# This approach allows:
#   - flexible query composition
#   - reuse across different analytical queries
#   - clean separation of SQL structure from Python logic
# ============================================================


# ------------------------------------------------------------
# parameters
#
# This is a LIST of table definitions.
#
# Ordering matters:
#   - The first entry is typically the "base" or fact table
#   - Subsequent entries are joined to earlier tables using
#     the provided `condition`
# ------------------------------------------------------------

parameters = [

    # --------------------------------------------------------
    # 1) Fact table (base table)
    #
    # - No join condition is required here
    # - Other tables will join to this table
    # --------------------------------------------------------
    {
        "table": "spotify.silver.fact_stream",
        "alias": "fact_stream",
        "cols": (
            "fact_stream.stream_id, "
            "fact_stream.listen_duration"
        ),
    },

    # --------------------------------------------------------
    # 2) User dimension
    #
    # - Joined to the fact table via user_id
    # - Provides user-level descriptive attributes
    # --------------------------------------------------------
    {
        "table": "spotify.silver.dim_user",
        "alias": "dim_user",
        "cols": (
            "dim_user.user_id, "
            "dim_user.user_name"
        ),
        "condition": "fact_stream.user_id = dim_user.user_id",
    },

    # --------------------------------------------------------
    # 3) Track dimension
    #
    # - Joined to the fact table via track_id
    # - Provides track-level descriptive attributes
    # --------------------------------------------------------
    {
        "table": "spotify.silver.dim_track",
        "alias": "dim_track",
        "cols": (
            "dim_track.track_id, "
            "dim_track.track_name"
        ),
        "condition": "fact_stream.track_id = dim_track.track_id",
    },
]


# COMMAND ----------

# MAGIC %md
# MAGIC ### Install Jinja Library

# COMMAND ----------

# ============================================================
# Environment setup: install Jinja2 and restart Python kernel
#
# Purpose
# -------
# Ensure the Jinja2 templating library is available in the
# current Databricks notebook environment.
#
# A Python restart is required so that:
#   - the newly installed package is picked up by the interpreter
#   - imports behave consistently across subsequent cells
#
# NOTE
# ----
# Restarting Python will:
#   - clear all in-memory Python variables
#   - require re-running earlier setup / configuration cells
#   - NOT affect data written to storage or tables
# ============================================================


# ------------------------------------------------------------
# Install the Jinja2 package
#
# - Uses pip inside the notebook environment
# - Safe to run multiple times (idempotent)
# ------------------------------------------------------------

!pip install jinja2


# ------------------------------------------------------------
# Restart the Python interpreter
#
# Required for the newly installed package to be available
# via standard `import jinja2` statements.
# ------------------------------------------------------------

%restart_python


# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Jinja Template

# COMMAND ----------

# ============================================================
# Import Jinja2 Template class
#
# Purpose
# -------
# Import the `Template` class from Jinja2, which is used to:
#   - define parameterised SQL templates
#   - render final SQL strings by injecting runtime values
#
# This import assumes:
#   - Jinja2 has been installed in the environment
#   - The Python interpreter has been restarted after installation
# ============================================================

from jinja2 import Template


# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Query

# COMMAND ----------

# ============================================================
# Jinja SQL template definition
#
# Purpose
# -------
# Define a parameterised SQL query using Jinja templating.
#
# This template dynamically constructs:
#   - the SELECT column list
#   - the FROM clause (base / fact table)
#   - a series of LEFT JOINs to dimension tables
#
# The actual SQL is rendered later by injecting a `parameters`
# structure (list of dictionaries) into this template.
# ============================================================


# ------------------------------------------------------------
# query_text
#
# This is a *raw SQL template*, not executable SQL yet.
# Jinja syntax ({% ... %}, {{ ... }}) is used to generate
# valid SQL once rendered.
#
# Expected input:
#   parameters: list of dicts, where each dict contains:
#     - table     : fully qualified table name
#     - alias     : table alias used in the query
#     - cols      : comma-separated column list
#     - condition : (optional) join condition
#
# Conventions:
#   - parameters[0] is treated as the BASE (fact) table
#   - parameters[1:] are treated as DIMENSION tables
#   - LEFT JOINs are used to avoid dropping fact rows
# ------------------------------------------------------------

query_text = """
SELECT
{% for param in parameters %}
    {{ param.cols }}{% if not loop.last %},{% endif %}
{% endfor %}
FROM {{ parameters[0].table }} AS {{ parameters[0].alias }}
{% for param in parameters[1:] %}
LEFT JOIN {{ param.table }} AS {{ param.alias }}
    ON {{ param.condition }}
{% endfor %}
"""


# ------------------------------------------------------------
# Template behaviour explained (plain English)
#
# 1) SELECT clause
#    - Iterates over all entries in `parameters`
#    - Emits each table's `cols`
#    - Adds commas between column groups, but not after the last
#
# 2) FROM clause
#    - Uses the FIRST entry in `parameters`
#    - Assumes this is the fact / base table
#
# 3) JOIN clauses
#    - Iterates over all remaining entries in `parameters`
#    - Emits a LEFT JOIN for each
#    - Uses the provided join condition
#
# Result
# ------
# After rendering, this template produces a valid SQL query
# that can be executed via spark.sql(query).
# ------------------------------------------------------------


# COMMAND ----------

# ============================================================
# Render parameterised SQL using Jinja templating
#
# Purpose
# -------
# Convert a Jinja-templated SQL string into a fully rendered
# SQL query by injecting runtime parameters.
#
# This is typically used when:
#   - SQL logic is stored as a template (e.g. in a file or string)
#   - Values such as dates, table names, or filters vary per run
# ============================================================


# ------------------------------------------------------------
# 1) Create a Jinja template from the raw SQL text
#
# query_text:
#   - A string containing Jinja placeholders, e.g.:
#       SELECT * FROM {{ parameters.table }}
#       WHERE date >= '{{ parameters.start_date }}'
# ------------------------------------------------------------

jinja_sql_str = Template(query_text)


# ------------------------------------------------------------
# 2) Render the template with runtime parameters
#
# parameters:
#   - Dictionary-like object containing values referenced
#     inside the SQL template
#
# Result:
#   - `query` is now a plain SQL string with all placeholders
#     resolved
# ------------------------------------------------------------

query = jinja_sql_str.render(parameters=parameters)


# ------------------------------------------------------------
# 3) Output the rendered SQL for visibility / debugging
#
# This is extremely useful for:
#   - Verifying parameter substitution
#   - Debugging unexpected query behaviour
#   - Logging the exact SQL sent to Spark
# ------------------------------------------------------------

print(query)


# COMMAND ----------

# ============================================================
# Execute rendered SQL query and display results
#
# Purpose
# -------
# Execute the fully rendered SQL string against Spark and
# display the result as a tabular output in the notebook.
#
# Assumptions
# -----------
# - `query` is a valid SQL string (already rendered from Jinja)
# - All referenced tables exist and are accessible
# - The SQL syntax is compatible with Spark SQL
# ============================================================


# ------------------------------------------------------------
# Execute the SQL query
#
# spark.sql(query):
#   - Submits the SQL string to the Spark SQL engine
#   - Returns a DataFrame representing the query result
#
# display(...):
#   - Renders the DataFrame in the Databricks notebook UI
# ------------------------------------------------------------

display(
    spark.sql(query)
)
