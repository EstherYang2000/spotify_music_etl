import spotipy
from spotipy.oauth2 import SpotifyClientCredentials,SpotifyOAuth
import pandas as pd
from lib.config_helper import IACConfigHelper
import argparse
import json




def spotify_auth(client_id:str,client_secret:str,redirect_uri:str,scope=None)->spotipy.Spotify:
    client_credentials_manager = SpotifyOAuth(client_id=client_id,
                                client_secret=client_secret,
                                redirect_uri=redirect_uri,
                                scope=scope,
                                cache_path='.spotipyoauthcache'
                                )
    sp = spotipy.Spotify(auth_manager = client_credentials_manager)
    return sp

def extract_top_artists(sp: spotipy.Spotify)->json:
    # Fetch the recently played tracks by the current user
    top_artists = sp.current_user_top_artists()
    processed_top_artists = []
    for item in top_artists['items']:
        processed_top_artists.append({
            "uri": item['uri'],
            "name": item['name'],
            "type": item['type'],
            "followers": item['followers']['total'],
            "popularity": item['popularity'],
            "genres": ', '.join(item['genres']),  # Joining list into a comma-separated string for display
            "spotify_url": item['external_urls']['spotify'],
            "image_url": item['images'][1]['url']  # Using the second image size as an example
        })
    return processed_top_artists
def extract_top_tracks(sp: spotipy.Spotify)->json:
    # Fetch the recently played tracks by the current user
    top_tracks = sp.current_user_top_tracks()
    processed_top_tracks = []
    for song in top_tracks['items']:
        processed_top_tracks.append({
            "song_name": song['name'],
            "artist": ", ".join(artist['name'] for artist in song['artists']),
            "album": song['album']['name'],
            "release_date": song['album']['release_date'],
            "total_tracks_in_album": song['album']['total_tracks'],
            "song_duration": song['duration_ms'],  # Joining list into a comma-separated string for display
            "popularity": song['popularity'],
            "spotify_album_url": song['album']['external_urls']['spotify'],
            "preview_url": song['preview_url'],  # Using the second image size as an example
            "image_url": song['album']['images'][1]['url']
        })
    return processed_top_tracks

def extract_current_follow_artists(sp: spotipy.Spotify)->dict:
    # Fetch the recently played tracks by the current user
    favorite_artists = sp.current_user_followed_artists()
    processed_favorite_artists = []
    for artist in favorite_artists['artists']['items']:
        processed_favorite_artists.append({
            "uri": artist['uri'],
            "name": artist['name'],
            "type": artist['type'],
            "followers": artist['followers']['total'],
            "popularity": artist['popularity'],
            "genres": ', '.join(artist['genres']),  # Joining list into a comma-separated string for display
            "spotify_url": artist['external_urls']['spotify'],
            "image_url": artist['images'][1]['url']  # Using the second image size as an example
        })
    return processed_favorite_artists



def extract_favorite_singer(sp: spotipy.Spotify, artist_name: str) -> pd.DataFrame:
    # Search for the artist to get the URI
    search_result = sp.search(q="artist:" + artist_name, type='artist', limit=1)
    artist_uri = search_result['artists']['items'][0]['uri']
    # Fetch albums by the artist
    albums = sp.artist_albums(artist_uri, album_type='album', limit=50)
    album_names = [album['name'] for album in albums['items']]
    album_uris = [album['uri'] for album in albums['items']]
    # Initialize lists to store song data
    song_names = []
    song_uris = []
    song_albums = []
    # Fetch songs from each album
    for album_uri, album_name in zip(album_uris, album_names):
        tracks = sp.album_tracks(album_uri)
        for track in tracks['items']:
            song_names.append(track['name'])
            song_uris.append(track['uri'])
            song_albums.append(album_name)
    # Initialize lists to store audio features
    features_list = ['danceability', 'acousticness', 'energy', 'instrumentalness', 'liveness', 'loudness', 'speechiness', 'tempo', 'valence', 'popularity']
    # Dictionary to collect the features
    features_data = {feature: [] for feature in features_list}
    # Batch requests for audio features to reduce API calls
    for i in range(0, len(song_uris), 50):
        batch_uris = song_uris[i:i+50]
        audio_features = sp.audio_features(batch_uris)
        for feature, track in zip(audio_features, batch_uris):
            if feature:  # Check if features are found
                for feature_name in features_list[:-1]:  # Exclude popularity for now
                    features_data[feature_name].append(feature[feature_name])
                
                # Fetch popularity separately
                track_data = sp.track(track)
                features_data['popularity'].append(track_data['popularity'])
            else:
                # Handle missing features
                for feature_name in features_list:
                    features_data[feature_name].append(None)

    # Create a DataFrame
    df = pd.DataFrame({
        'name': song_names,
        'album': song_albums,
        **features_data
    })

    return df




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract data from Spotify.")
    parser.add_argument(
        "--config", type=str, default="default_value", help="environment"
    )
    args = parser.parse_args()
    conn_config = IACConfigHelper.get_conn_info(args.config)
    sp = spotify_auth(conn_config['spotify']['client_id'],conn_config['spotify']['client_secret'],conn_config['spotify']['redirect_url'],conn_config['spotify']['scope'])
    # print(sp)
    processed_top_artists_json = extract_top_artists(sp)
    print(processed_top_artists_json) 
    processed_top_tracks_json = extract_top_tracks(sp)
    print(processed_top_tracks_json)
    processed_current_follow_artists_json = extract_current_follow_artists(sp)
    print(processed_current_follow_artists_json)
    
    # # Example usage (ensure you have your Spotify API credentials set up)
    # client_credentials_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET,requests_timeout=30)
    # sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    # artist_name = "Taylor Swift"
    # df = extract_favorite_singer(sp, artist_name)
    # print(df.head())