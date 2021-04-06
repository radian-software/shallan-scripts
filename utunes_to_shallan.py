#!/usr/bin/env python3


import abc
import pathlib
import subprocess
import sys
import uuid


class Column(abc.ABC):
    def __init__(self, name, nullable=False, primary=False, references=None):
        self.name = name
        self.nullable = nullable
        self.primary = primary
        self.references = references

    @abc.abstractmethod
    def get_type(self):
        pass

    def get_constraint(self):
        return None

    def __str__(self):
        stmt = f"{self.name} {self.get_type()}"
        if not self.nullable:
            stmt += " NOT NULL"
        if self.primary:
            stmt += " PRIMARY KEY"
        cons = self.get_constraint()
        if cons:
            stmt += f" CHECK ({cons})"
        if self.references:
            stmt += f", FOREIGN KEY({self.name}) REFERENCES {self.references}"
        return stmt


class TextColumn(Column):
    def get_type(self):
        return "TEXT"

    def get_constraint(self):
        return f"{self.name} != ''"


class BoolColumn(Column):
    def get_type(self):
        return "INTEGER"

    def get_constraint(self):
        return f"{self.name} IN (0, 1)"


class HashColumn(Column):
    def get_type(self):
        return "TEXT"

    def get_constraint(self):
        return f"{self.name} GLOB '{'[0-9a-f]' * 40}'"


class URLColumn(Column):
    def get_type(self):
        return "TEXT"

    def get_constraint(self):
        return f"{self.name} GLOB 'http://*' OR {self.name} GLOB 'https://*'"


class NumColumn(Column):
    def get_type(self):
        return "INTEGER"

    def get_constraint(self):
        return f"{self.name} >= 1"


class PriceColumn(Column):
    def get_type(self):
        return "INTEGER"

    def get_constraint(self):
        return f"{self.name} >= 0"


class DateColumn(Column):
    def get_type(self):
        return "TEXT"

    def get_constraint(self):
        return f"{self.name} GLOB '[12][0-9][0-9][0-9]-[01][0-9]-[0-3][0-9]'"


class YearColumn(Column):
    def get_type(self):
        return "TEXT"

    def get_constraint(self):
        return f"{self.name} GLOB '[12][0-9][0-9][0-9]'"


class TimestampColumn(Column):
    def get_type(self):
        return "INTEGER"

    def get_constraint(self):
        # Between 1000 CE and 3000 CE seems reasonable
        return f"{self.name} > -30610195622000 AND {self.name} < 32503708800000"


class ImageExtColumn(Column):
    def get_type(self):
        return "TEXT"

    def get_constraint(self):
        return f"{self.name} IN ('jpg', 'png', 'tif')"


class Table:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns

    def __str__(self):
        args = ",\n  ".join(map(str, self.columns))
        return f"""
CREATE TABLE {self.name} (
  {args}
);
""".strip()


def main():
    shallan_scripts = pathlib.Path(__file__).resolve().parent
    assert shallan_scripts.name == "shallan-scripts"
    music_dir = shallan_scripts.parent
    utunes_lib = music_dir / "utunes"
    shallan_lib = music_dir / "shallan-lib"
    database = shallan_lib / "library.sqlite3"
    sql_script = shallan_scripts / "utunes_to_shallan.sql"
    stmts = []
    song_columns = [
        HashColumn("id", primary=True),
        BoolColumn("acquired_illegally"),
        BoolColumn("acquired_legally"),
        TextColumn("album"),
        TextColumn("album_artist"),
        TextColumn("album_artist_sort"),
        TextColumn("album_sort"),
        TextColumn("artist"),
        TextColumn("artist_sort"),
        HashColumn("artwork"),
        ImageExtColumn("artwork_ext"),
        BoolColumn("as_bundle"),
        BoolColumn("as_gift"),
        TextColumn("composer"),
        TextColumn("composer_sort"),
        DateColumn("date_added"),
        NumColumn("disc"),
        HashColumn("media"),
        TextColumn("purchase_group", nullable=True),
        PriceColumn("min_price_cents"),
        PriceColumn("paid_cents"),
        URLColumn("refined_source", nullable=True),
        TextColumn("name"),
        TextColumn("name_sort"),
        URLColumn("source"),
        NumColumn("track"),
        URLColumn("tracklist", nullable=True),
        YearColumn("year_released"),
    ]
    stmts.append(Table("songs", song_columns))
    play_columns = [
        HashColumn("id", primary=True),
        TimestampColumn("timestamp_ms"),
        HashColumn("song_id", references="songs"),
    ]
    stmts.append(Table("plays", play_columns))
    sql = "\n".join(map(str, stmts)) + "\n"
    with open(sql_script, "w") as f:
        f.write(sql)
    try:
        database.unlink()
    except FileNotFoundError:
        pass
    result = subprocess.run(["sqlite3", str(database)], input=sql.encode())
    if result.returncode != 0:
        sys.exit(result.returncode)
    sys.exit(0)


if __name__ == "__main__":
    main()
