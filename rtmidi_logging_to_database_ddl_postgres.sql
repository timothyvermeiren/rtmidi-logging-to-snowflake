-- Only once eh
CREATE DATABASE rtmltd;
CREATE USER rtmltd WITH ENCRYPTED PASSWORD 'hunter2';
GRANT ALL PRIVILEGES ON DATABASE rtmltd to rtmltd;
GRANT ALL ON SCHEMA rtmltd.public TO rtmltd; 

-- The tables and views.
CREATE TABLE MIDI_DRUMS_RAW (
	V VARCHAR(65535)
);

CREATE TABLE MIDI_TD4_DRUMS_MAPPING (
	"value" VARCHAR(65535),
	"pad_name" VARCHAR(65535),
	"pad_zone" VARCHAR(65535),
	"pad_status" VARCHAR(65535)
);

INSERT INTO MIDI_TD4_DRUMS_MAPPING ("value",pad_name,pad_zone,pad_status)
VALUES
('G#1','Hi-Hat','Pedal','Closing'),
('A#-1','Hi-Hat','Edge','Closed'),
('D0','Hi-Hat','Edge','Open'),
('F#1','Hi-Hat','Bow','Closed'),
('A#1','Hi-Hat','Bow','Open'),
('D1','Snare','Head',''),
('E1','Snare','Rim',''),
('C2','Tom Hi','',''),
('A1','Tom Mid','',''),
('F1','Tom Lo','',''),
('G2','Crash','Edge',''),
('C#2','Crash','Bow',''),
('E2','China','Edge',''),
('A2','China','Bow',''),
('B2','Ride','Edge',''),
('D#2','Ride','Bow',''),
('C1','Kick','','')
;

CREATE VIEW TV_MIDI_DRUMS_NOTES(
	MIDI_DATA_TYPE,
	TIMESTAMP,
	MIDI_NOTE,
	DRUM_PAD
) AS (
    SELECT
    dr.v::json->>'midi-data-type'::varchar AS midi_data_type,
    '1970-01-01'::date + (((dr.v::json->>'timestamp')::float * 1000)::bigint * INTERVAL '1 millisecond') AS "timestamp",
    dr.v::json->>'value'::varchar AS midi_note,
    CASE dm."pad_zone" IS NOT NULL
        WHEN TRUE THEN
            CASE dm."pad_status" IS NOT NULL
                WHEN TRUE THEN
                    dm."pad_name" || ' ' || dm."pad_zone" || ' ' || dm."pad_status"
                ELSE
                    dm."pad_name" || ' ' || dm."pad_zone"
            END
        ELSE
            dm."pad_name"
    END AS drum_pad,
    dr.v AS "raw_data"
    FROM MIDI_DRUMS_RAW dr
    LEFT JOIN MIDI_TD4_DRUMS_MAPPING dm ON dr.v::json->>'value'::varchar = dm."value"
    WHERE dr.v::json->>'midi-data-type'::varchar = 'note on'
);

CREATE VIEW TV_MIDI_DRUMS_NOTES_CLUSTERED AS (

	WITH event_data AS (
	    SELECT 
	        *
	    FROM 
	        tv_midi_drums_notes
	    ORDER BY 
	        "timestamp"
	),
	time_diffs AS (
	    SELECT
	        "timestamp",
	        midi_data_type,
	        midi_note,
	        drum_pad,
	        raw_data,
	        LAG("timestamp") OVER (ORDER BY "timestamp") AS prev_event_timestamp
	    FROM
	        event_data
	),
	cluster_marks AS (
	    SELECT
	        "timestamp",
	        midi_data_type,
	        midi_note,
	        drum_pad,
	        raw_data,
	        prev_event_timestamp,
	        CASE 
	            WHEN prev_event_timestamp IS NULL THEN 0
	            WHEN "timestamp" - prev_event_timestamp > INTERVAL '15 minutes' THEN 1  -- Adjust threshold as needed
	            ELSE 0
	        END AS is_new_cluster
	    FROM
	        time_diffs
	),
	clusters AS (
	    SELECT
	        "timestamp",
	        midi_data_type,
	        midi_note,
	        drum_pad,
	        raw_data,
	        SUM(is_new_cluster) OVER (ORDER BY "timestamp") AS cluster_id
	    FROM
	        cluster_marks
	)
	SELECT
	    "timestamp",
	    midi_data_type,
	    midi_note,
	    drum_pad,
	    raw_data,
	    cluster_id AS session_cluster
	FROM
	    clusters
	ORDER BY
	    "timestamp"
)	
;
