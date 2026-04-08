import sqlite3
import os


def build_database(conn):
    """Create the four-table music schema in the database referenced by conn."""
    conn.execute("PRAGMA foreign_keys = ON;")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS Artist (
            artist_id    INTEGER PRIMARY KEY,
            name         TEXT    NOT NULL,
            genre        TEXT    NOT NULL,
            origin_city  TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS Track (
            track_id         INTEGER PRIMARY KEY,
            title            TEXT    NOT NULL,
            duration_seconds INTEGER NOT NULL,
            artist_id        INTEGER NOT NULL
                REFERENCES Artist(artist_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS Playlist (
            playlist_id    INTEGER PRIMARY KEY,
            playlist_name  TEXT    NOT NULL,
            owner_name     TEXT    NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS PlaylistTrack (
            playlist_id  INTEGER NOT NULL REFERENCES Playlist(playlist_id),
            track_id     INTEGER NOT NULL REFERENCES Track(track_id),
            position     INTEGER NOT NULL,
            PRIMARY KEY (playlist_id, track_id)
        )
    """)

    conn.commit()
    print("build_database: schema created successfully.")


def seed_database(conn):
    """Populate all four tables with realistic music data."""

    artists = [
        (1, "Drake", "Hip-Hop", "Toronto"),
        (2, "Kendrick Lamar", "Hip-Hop", "Compton"),
        (3, "J. Cole", "Hip-Hop", "Fayetteville"),
        (4, "Travis Scott", "Hip-Hop", "Houston"),
        (5, "Future", "Hip-Hop", "Atlanta"),
        (6, "A$AP Rocky", "Hip-Hop", "New York"),
    ]

    conn.executemany(
        "INSERT OR IGNORE INTO Artist VALUES (?, ?, ?, ?)",
        artists
    )

    tracks = [
        (1, "God's Plan", 198, 1),
        (2, "One Dance", 173, 1),
        (3, "Nonstop", 238, 1),
        (4, "HUMBLE.", 177, 2),
        (5, "DNA.", 185, 2),
        (6, "Money Trees", 386, 2),
        (7, "No Role Modelz", 292, 3),
        (8, "Middle Child", 213, 3),
        (9, "Wet Dreamz", 239, 3),
        (10, "SICKO MODE", 312, 4),
        (11, "Goosebumps", 244, 4),
        (12, "Highest in the Room", 175, 4),
        (13, "Mask Off", 204, 5),
        (14, "Life Is Good", 237, 5),
        (15, "March Madness", 244, 5),
        (16, "Praise The Lord", 205, 6),
        (17, "L$D", 238, 6),
        (18, "Fashion Killa", 231, 6),
    ]

    conn.executemany(
        "INSERT OR IGNORE INTO Track VALUES (?, ?, ?, ?)",
        tracks
    )

    playlists = [
        (1, "Gym Hits", "Ilyas"),
        (2, "Late Night Drive", "Alex"),
        (3, "Study Session", "Sam"),
        (4, "Weekend Vibes", "Jordan"),
    ]

    conn.executemany(
        "INSERT OR IGNORE INTO Playlist VALUES (?, ?, ?)",
        playlists
    )

    playlist_tracks = [
        (1, 1, 1),
        (1, 4, 2),
        (1, 8, 3),
        (1, 10, 4),
        (1, 13, 5),

        (2, 2, 1),
        (2, 6, 2),
        (2, 11, 3),
        (2, 17, 4),
        (2, 18, 5),

        (3, 5, 1),
        (3, 7, 2),
        (3, 9, 3),
        (3, 14, 4),
        (3, 16, 5),

        (4, 3, 1),
        (4, 4, 2),
        (4, 10, 3),
        (4, 12, 4),
        (4, 15, 5),
    ]

    conn.executemany(
        "INSERT OR IGNORE INTO PlaylistTrack VALUES (?, ?, ?)",
        playlist_tracks
    )

    conn.commit()
    print("seed_database: data inserted successfully.")


if __name__ == "__main__":

    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON;")
    build_database(conn)
    seed_database(conn)

    row_counts = {
        "Artist": conn.execute("SELECT COUNT(*) FROM Artist").fetchone()[0],
        "Track": conn.execute("SELECT COUNT(*) FROM Track").fetchone()[0],
        "Playlist": conn.execute("SELECT COUNT(*) FROM Playlist").fetchone()[0],
        "PlaylistTrack": conn.execute("SELECT COUNT(*) FROM PlaylistTrack").fetchone()[0],
    }

    print("\nRow counts after seeding:")
    for table, count in row_counts.items():
        print(f"  {table:<16} {count:>3} rows")

    print("\nIntegrityError demonstration:")
    try:
        conn.execute(
            "INSERT INTO Track VALUES (?, ?, ?, ?)",
            (999, "Ghost Track", 210, 9999)
        )
        print("  Insert succeeded — did you enable PRAGMA foreign_keys = ON?")
    except sqlite3.IntegrityError as e:
        conn.rollback()
        print(f"  IntegrityError caught: {e}")
        print("  This error confirms that foreign key enforcement is active.")

    print("\nPersisting database to music.db ...")
    DB_PATH = "music.db"

    try:
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)

        target_conn = sqlite3.connect(DB_PATH)
        conn.backup(target_conn)
        target_conn.commit()
        target_conn.close()

        print(f"  Backup complete. File size: {os.path.getsize(DB_PATH):,} bytes")
        print(f"  Reopen with: sqlite3.connect('{DB_PATH}')")

    except Exception as e:
        print(f"  Backup failed: {type(e).__name__}: {e}")

    finally:
        conn.close()