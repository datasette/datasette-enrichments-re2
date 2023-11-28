from datasette import hookimpl
import sqlite_utils
import json
from datasette_enrichments import Enrichment
import re2

from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from datasette.app import Datasette
    from datasette.database import Database

from wtforms import SelectField, Form, TextAreaField, StringField
from wtforms.validators import DataRequired, ValidationError


@hookimpl
def register_enrichments(datasette):
    return [Re2Enrichment()]


_re_named_group = re2.compile(r"\(\?P<\w+>")


def has_named_groups(pattern_string):
    return _re_named_group.search(pattern_string) is not None


class Re2Enrichment(Enrichment):
    name = "Regular expressions"
    slug = "re2"
    description = "Extract data into new columns using regular expressions"

    async def get_config_form(self, db: "Database", table: str):
        columns = await db.table_columns(table)

        class ConfigForm(Form):
            source_column = SelectField(
                "Source column",
                choices=[(column, column) for column in columns],
                validators=[DataRequired(message="A column is required.")],
            )
            mode = SelectField(
                "Capture mode",
                choices=[
                    ("replace", "Search and replace "),
                    ("single", "Store first match in a single column"),
                    ("json", "Store all matches as JSON in a single column"),
                    ("multi", "Use named capture groups and store in multiple columns"),
                ],
                validators=[DataRequired(message="A mode is required.")],
            )
            regex = TextAreaField(
                "Regular expression",
                validators=[DataRequired(message="A regular expression is required.")],
            )
            replacement = StringField(
                "Replacement",
            )
            single_column = StringField(
                "Single output column",
            )

            # Custom validator, single_column must be set if choice is single
            def validate_single_column(form, field):
                if form.mode.data in ("single", "json") and not field.data:
                    raise ValidationError("A single output column is required.")

            # If mode is "multi" confirm regex has named capture groups
            def validate_regex(form, field):
                if form.mode.data == "multi" and not has_named_groups(field.data):
                    raise ValidationError(
                        "Regular expression must contain named capture groups."
                    )

            # if mode is "replace" confirm replacement is set
            def validate_replacement(form, field):
                if form.mode.data == "replace" and not field.data:
                    raise ValidationError("A replacement is required.")

        return ConfigForm

    async def enrich_batch(
        self,
        datasette: "Datasette",
        db: "Database",
        table: str,
        rows: List[dict],
        pks: List[str],
        config: dict,
        job_id: int,
    ):
        pattern = config["regex"]
        re = re2.compile(pattern)
        source_column = config["source_column"]
        single_column = config["single_column"]

        if not single_column and config["mode"] == "replace":
            single_column = source_column

        if not pks:
            pks = ["rowid"]

        is_named_groups = has_named_groups(pattern)

        to_update = []

        if config["mode"] == "multi":
            for row in rows:
                ids = [row[pk] for pk in pks]
                match = re.search(row[source_column])
                if match is not None:
                    to_update.append((ids, match.groupdict()))
        elif config["mode"] == "single":
            for row in rows:
                match = re.search(row[source_column])
                if match is not None:
                    ids = [row[pk] for pk in pks]
                    to_update.append((ids, {single_column: match.group(1)}))
        elif config["mode"] == "json":
            for row in rows:
                if is_named_groups:
                    matches = [m.groupdict() for m in re.finditer(row[source_column])]
                else:
                    matches = list(re.findall(row[source_column]))
                if matches:
                    ids = [row[pk] for pk in pks]
                    to_update.append((ids, {single_column: json.dumps(matches)}))
        elif config["mode"] == "replace":
            for row in rows:
                ids = [row[pk] for pk in pks]
                to_update.append(
                    (
                        ids,
                        {
                            single_column: re.sub(
                                config["replacement"], row[source_column]
                            )
                        },
                    )
                )

        if to_update:

            def fn(conn):
                db = sqlite_utils.Database(conn)
                for ids, values in to_update:
                    db[table].update(ids, values, alter=True)

            await db.execute_write_fn(fn, block=True)
