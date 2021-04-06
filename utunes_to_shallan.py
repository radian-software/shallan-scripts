#!/usr/bin/env python3


import abc
import decimal
import hashlib
import json
import pathlib
import random
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
            stmt += f" CONSTRAINT {self.name} CHECK ({cons})"
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


class IdColumn(Column):
    def get_type(self):
        return "TEXT"

    def get_constraint(self):
        return f"{self.name} GLOB '{'[0-9a-f]' * 32}'"


class HashColumn(Column):
    def get_type(self):
        return "TEXT"

    def get_constraint(self):
        return f"{self.name} GLOB '{'[0-9a-f]' * 64}'"


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
        return "INTEGER"

    def get_constraint(self):
        return f"{self.name} >= 1000 AND {self.name} < 3000"


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


def from_yesno(val):
    if val == "yes":
        return 1
    if val == "no":
        return 0
    assert False, val


def hash_file(path):
    m = hashlib.sha256()
    with open(path, "rb") as f:
        m.update(f.read())
    return m.hexdigest()


def get_uuid():
    return str(uuid.uuid4()).replace("-", "")


def utunes_song_to_fields(song, utunes_lib):
    print(song["filename"], file=sys.stderr)
    artwork_ext = pathlib.Path(song.get("artwork")).suffix.lstrip(".")
    if artwork_ext == "jpeg":
        artwork_ext = "jpg"
    if artwork_ext == "tiff":
        artwork_ext = "tif"
    return {
        "id": get_uuid(),
        "acquired_illegally": from_yesno(song.get("acquired_illegally")),
        "acquired_legally": from_yesno(song.get("acquired_legally")),
        "album": song.get("album"),
        "album_artist": song.get("album_artist"),
        "album_artist_sort": song.get("album_artist_sort") or song.get("album_artist"),
        "album_sort": song.get("album_sort") or song.get("album"),
        "artist": song.get("artist"),
        "artist_sort": song.get("artist_sort") or song.get("artist"),
        "artwork_hash": hash_file(utunes_lib / "artwork" / song.get("artwork")),
        "artwork_ext": artwork_ext,
        "as_bundle": from_yesno(song.get("as_bundle")),
        "as_gift": from_yesno(song.get("as_gift")),
        "composer": song.get("composer"),
        "composer_sort": song.get("composer_sort") or song.get("composer"),
        "date_added": song.get("date"),
        "disc": int(song.get("disc")),
        "song_hash": hash_file(utunes_lib / "music" / song.get("filename")),
        "purchase_group": song.get("group"),
        "min_price_cents": int(decimal.Decimal(song.get("min_price") or "0.00") * 100),
        "paid_cents": int(decimal.Decimal(song.get("paid") or "0.00") * 100),
        "refined_source": song.get("refined_source"),
        "name": song.get("song"),
        "name_sort": song.get("song_sort") or song.get("song"),
        "source": song.get("source"),
        "track": song.get("track") and int(song.get("track")),
        "tracklist": song.get("tracklist"),
        "year_released": int(song.get("year")),
        "utunes_id": song.get("id"),
    }


def main():
    shallan_scripts = pathlib.Path(__file__).resolve().parent
    assert shallan_scripts.name == "shallan-scripts"
    music_dir = shallan_scripts.parent
    utunes_lib = music_dir / "utunes"
    shallan_lib = music_dir / "shallan-lib"
    utunes_json = utunes_lib / "utunes.json"
    shallan_db = shallan_lib / "library.sqlite3"
    sql_script = shallan_scripts / "utunes_to_shallan.sql"
    stmts = []
    song_columns = [
        IdColumn("id", primary=True),
        BoolColumn("acquired_illegally"),
        BoolColumn("acquired_legally"),
        TextColumn("album"),
        TextColumn("album_artist"),
        TextColumn("album_artist_sort"),
        TextColumn("album_sort"),
        TextColumn("artist"),
        TextColumn("artist_sort"),
        HashColumn("artwork_hash"),
        ImageExtColumn("artwork_ext"),
        BoolColumn("as_bundle"),
        BoolColumn("as_gift"),
        TextColumn("composer", nullable=True),
        TextColumn("composer_sort", nullable=True),
        DateColumn("date_added"),
        NumColumn("disc"),
        HashColumn("song_hash"),
        TextColumn("purchase_group", nullable=True),
        PriceColumn("min_price_cents"),
        PriceColumn("paid_cents"),
        URLColumn("refined_source", nullable=True),
        TextColumn("name"),
        TextColumn("name_sort"),
        URLColumn("source"),
        NumColumn("track", nullable=True),
        URLColumn("tracklist", nullable=True),
        YearColumn("year_released"),
        TextColumn("utunes_id", nullable=True),
    ]
    stmts.append(Table("songs", song_columns))
    play_columns = [
        IdColumn("id", primary=True),
        TimestampColumn("timestamp_ms"),
        IdColumn("song_id", references="songs"),
    ]
    stmts.append(Table("plays", play_columns))
    with open(utunes_json) as f:
        utunes_data = json.load(f)
    songs = list(utunes_data["songs"].values())
    for song in songs:
        fields = utunes_song_to_fields(song, utunes_lib)
        col_names = [col.name for col in song_columns]
        vals = [fields[name] for name in col_names]
        str_vals = []
        for val in vals:
            if val is None:
                str_vals.append("NULL")
            elif isinstance(val, str):
                val = val.replace("'", "''")
                str_vals.append(f"'{val}'")
            else:
                str_vals.append(repr(val))
        stmts.append(
            f"""
INSERT INTO songs ({', '.join(col_names)})
VALUES ({', '.join(str_vals)});
""".strip()
        )
    sql = "\n".join(map(str, stmts)) + "\n"
    with open(sql_script, "w") as f:
        f.write(sql)
    try:
        shallan_db.unlink()
    except FileNotFoundError:
        pass
    print(f"% sqlite3 {shallan_db} < {sql_script}", file=sys.stderr)
    result = subprocess.run(["sqlite3", str(shallan_db)], input=sql.encode())
    if result.returncode != 0:
        sys.exit(result.returncode)
    sys.exit(0)


if __name__ == "__main__":
    main()
