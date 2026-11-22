# tjts

A timeshifting radio proxy. It records ABC's live HLS streams (Triple J, Double J) and replays them shifted to the listener's timezone, so breakfast radio plays at breakfast wherever you are.

## Endpoints

All take `stream=<id>&tz=<IANA timezone>`, e.g. `?stream=triplej&tz=Europe/Berlin`.

| Path | What |
|------|------|
| `/` | Web player with now-playing and track history |
| `/icecast` | Plain AAC stream with ICY metadata. Use this for Sonos, VLC, etc. |
| `/m3u8` | HLS playlist |
| `/nowplaying` | JSON: current track and recent history on the listener's timeline |

## Now playing

Streams with a `nowPlaying` block get track info from ABC's public play log (`music.abcradio.net.au/api/v1/plays`):

```yaml
streams:
  - id: triplej
    # ...
    nowPlaying:
      station: triplej   # ABC service id
      lag: 20s           # how far recorded audio trails the play log
```

- A poller fetches plays every minute and stores them per stream in S3 at `_meta/plays/<stream>.json`. On startup it loads that file, then backfills any gap from the API (which supports time ranges), so no separate backfill step is needed.
- Each chunk's fetch time minus `lag` gives the broadcast time of its audio. The current track is the play on air at that moment, so titles follow each listener's offset.
- `/icecast` sends `StreamTitle='Artist - Title'` every 16000 bytes when the client sends `Icy-MetaData: 1`. Between tracks it sends the station name.
- `lag` is tuned by ear. If titles change before the song does, increase it.

## Running locally

`docker compose up` starts MinIO, then:

```
go run . -config config.yaml
```
