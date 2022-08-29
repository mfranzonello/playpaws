# PlayPaws
Analyzer App for MusicLeague created by Michael Franzonello

## What It Is

<img src="https://www.dropbox.com/s/u4t1p0xz5qcs5ia/musicleague.png?raw=1" width="200" height="200"/>

This app takes data from a variety of sources to show you how you and your friends are doing in the [MusicLeague](https://app.musicleague.com/) app, giving insight into how you and your friends listen and vote together. You can see the results **[here](https://mfranzonello-playpaws-play-rzhfts.streamlitapp.com/)**.

## What Powers the Backend

### Overall Framework

#### Python

<img src="https://www.dropbox.com/s/5av6c31zg1xxk6i/python.png?raw=1" width="200" height="200"/>

All the code is written in [Python 3](https://www.python.org/), and using a fair amount of math to calculate not just who wins each round, but how closely related the individual players are to each other and the group.

#### Github

<img src="https://www.dropbox.com/s/qe27d4l16f3383o/github.png?raw=1" width="200" height="200"/>

Obviously, this app uses Github as a code repository, since that's where you're reading this, but it also uses [Github Actions](https://github.com/features/actions) to run at scheduled intervals based on a YAML file, to keep all the data up to date.

### Storage

#### bit.io

<img src="https://www.dropbox.com/s/xoobngrgjsoklw4/bitio.png?raw=1" width="200" height="200"/>

The main data comes from webscraping or zip file extraction from musicleage.app. That data is then stored in [bit.io](https://bit.io/mfranzonello/playpaws), a Postgres-based database, and updated with additional analytics and details resulting from all the other pieces of code.

#### Dropbox

<img src="https://www.dropbox.com/s/0xume6363vhx2l8/dropbox.png?raw=1" width="200" height="200"/>

Any images like profile pictures or league logos are stored in [Dropbox](https://www.dropbox.com/lp/developers), using its API for transfer.

### Enhancing Data

#### Spotify

<img src="https://www.dropbox.com/s/6o3bn708bg2zils/spotify.png?raw=1" width="200" height="200"/>

The [Spotify API](https://developer.spotify.com/documentation/web-api/) is used to get detailed information on player profiles, albums, artists, and tracks. Additionally, through the [MobiMusic](https://open.spotify.com/user/12125687151?si=64536d1157d642e9) account, it automatically creates and updates playlists after each round is finished: a comprehensive list of all tracks, a Best Of list of the most popular songs, and one list for each player's favorite tracks.

#### Last.fm

<img src="https://www.dropbox.com/s/86wrhqokk4ikaxf/lastfm.png?raw=1" width="200" height="200"/>

[Last.fm](https://www.last.fm/api) has some more data that Spotify doesn't provide around genre tags and popularity, so it's API is used to fill out some gaps.

#### Google Cloud Platform

<img src="https://www.dropbox.com/s/fj4yn82h2cbbe0b/gcp.png?raw=1" width="200" height="200"/>

For the rare times that album cover images aren't on Spotify or Last.fm, this app uses the [GCP](https://cloud.google.com/apis/docs/overview) API to search Google Images for a best match.

## What Powers the Frontend

#### Streamlit

<img src="https://www.dropbox.com/s/w398q75lbai63zk/streamlit.png?raw=1" width="200" height="200"/>

Used by many data scientists, [Streamlit](https://streamlit.io/) is a quick framework for displaying stats. To use this, you first select which player you want to see, and then which league (if they're playing in more than one).

#### Pillow, Wordcloud and FuzzyWuzzy

<img src="https://www.dropbox.com/s/76rke37rftotwn3/pillow.png?raw=1" width="200" height="200"/>

A few additional modules are also used for custom graphics tailor made for this app, including custom code that does some basic color analysis pulled from a previous project, [Ocelot](https://github.com/mfranzonello/ocelot).
