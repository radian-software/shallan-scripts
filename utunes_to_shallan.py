#!/usr/bin/env python3


import abc
import decimal
import hashlib
import json
import os
import pathlib
import random
import shutil
import subprocess
import sys
import uuid


class Column(abc.ABC):
    def __init__(
        self, name, nullable=False, primary=False, unique=False, references=None
    ):
        self.name = name
        self.nullable = nullable
        self.primary = primary
        self.unique = unique
        self.references = references

    @abc.abstractmethod
    def get_type(self):
        pass

    def get_constraint(self):
        return None

    def get_default(self):
        return None

    def __str__(self):
        stmt = f"{self.name} {self.get_type()} CONSTRAINT {self.name}"
        if not self.nullable:
            stmt += " NOT NULL"
        if self.primary:
            stmt += " PRIMARY KEY"
        if self.unique:
            stmt += " UNIQUE"
        stmt += f" CHECK (TYPEOF({self.name}) IN ('{self.get_type().lower()}', 'null'))"
        cons = self.get_constraint()
        if cons:
            stmt += f" CHECK ({cons})"
        default = self.get_default()
        if default:
            stmt += f" DEFAULT ({default})"
        if self.references:
            stmt += f" REFERENCES {self.references}"
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

    def get_default(self):
        if self.primary:
            return "lower(hex(randomblob(16)))"
        return super().get_default()


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


class Constraint(abc.ABC):
    def __init__(self, name):
        self.name = name

    @abc.abstractmethod
    def get_sql(self):
        pass

    def __str__(self):
        return f"CONSTRAINT {self.name} {self.get_sql()}"


class UniqueConstraint(Constraint):
    def __init__(self, name, columns):
        super().__init__(name)
        self.columns = columns

    def get_sql(self):
        return f"UNIQUE ({', '.join(self.columns)})"


class Table:
    def __init__(self, name, columns, constraints=[]):
        self.name = name
        self.columns = columns
        self.constraints = constraints

    def __str__(self):
        args = ",\n  ".join(map(str, self.columns + self.constraints))
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
    artwork_ext = pathlib.Path(song.get("artwork")).suffix.lstrip(".")
    if artwork_ext == "jpeg":
        artwork_ext = "jpg"
    if artwork_ext == "tiff":
        artwork_ext = "tif"
    artwork_file = utunes_lib / "artwork" / song.get("artwork")
    song_file = utunes_lib / "music" / song.get("filename")
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
        "artwork_hash": hash_file(artwork_file),
        "artwork_ext": artwork_ext,
        "as_bundle": from_yesno(song.get("as_bundle")),
        "as_gift": from_yesno(song.get("as_gift")),
        "composer": song.get("composer"),
        "composer_sort": song.get("composer_sort") or song.get("composer"),
        "date_added": song.get("date"),
        "disc": int(song.get("disc")),
        "song_hash": hash_file(song_file),
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
        "_artwork_file": artwork_file,
        "_song_file": song_file,
    }


def link_object(obj_hash, link_target, shallan_objects):
    path = shallan_objects / obj_hash[:2] / obj_hash[2:]
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        link_target.link_to(path)
    except FileExistsError:
        pass


def main(*, fast):
    shallan_scripts = pathlib.Path(__file__).resolve().parent
    assert shallan_scripts.name == "shallan-scripts"
    music_dir = shallan_scripts.parent
    utunes_lib = music_dir / "utunes"
    shallan_lib = music_dir / "shallan-lib"
    utunes_json = utunes_lib / "utunes.json"
    shallan_db = shallan_lib / "library.sqlite3"
    shallan_objects = shallan_lib / "objects"
    sql_script = shallan_scripts / "utunes_to_shallan.sql"
    stmts = ["PRAGMA foreign_keys = yes;"]
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
    song_constraints = [UniqueConstraint("song_unique", ["album", "name"])]
    stmts.append(Table("songs", song_columns, song_constraints))
    play_columns = [
        IdColumn("id", primary=True),
        TimestampColumn("timestamp_ms"),
        IdColumn("song_id", references="songs"),
    ]
    stmts.append(Table("plays", play_columns))
    playlist_columns = [
        IdColumn("id", primary=True),
        TextColumn("name", unique=True),
        NumColumn("song_index", nullable=True),
    ]
    stmts.append(Table("playlists", playlist_columns))
    playlist_song_columns = [
        IdColumn("id", primary=True),
        IdColumn("song_id", references="songs"),
        IdColumn("playlist_id", references="playlists"),
        NumColumn("song_index"),
    ]
    stmts.append(Table("playlist_songs", playlist_song_columns))
    play_queue_columns = [
        IdColumn("id", primary=True),
        TextColumn("device", unique=True),
        IdColumn("playlist_id", references="playlists"),
    ]
    stmts.append(Table("play_queues", play_queue_columns))
    journal_columns = [
        IdColumn("id", primary=True),
        TextColumn("txn"),
        TimestampColumn("timestamp_ms"),
    ]
    stmts.append(Table("journal", journal_columns))
    with open(utunes_json) as f:
        utunes_data = json.load(f)
    songs = list(utunes_data["songs"].values())
    if fast:
        songs = [song for song in songs if song["album"] == "evermore"]
    try:
        shutil.rmtree(shallan_objects)
    except FileNotFoundError:
        pass
    for idx, song in enumerate(songs, start=1):
        print(f"[{idx:5d}/{len(songs)}] {song['filename']}", file=sys.stderr)
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
        link_object(fields["artwork_hash"], fields["_artwork_file"], shallan_objects)
        link_object(fields["song_hash"], fields["_song_file"], shallan_objects)
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
    main(fast=os.getenv("FAST") not in (None, "0", "no"))
