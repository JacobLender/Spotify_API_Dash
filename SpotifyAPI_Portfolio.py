#!/usr/bin/env python
# coding: utf-8

# In[2]:


import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd


# In[4]:


# Spotify API Example Credentials (Use your Credentials from API setup)
CLIENT_ID = "cd3t4be42g62bbsd3454gsd34dg4363"  # Replace with your Spotify Client ID
CLIENT_SECRET = "23befb5fewg2334dfd43ggsew1524twe"  # Replace with your Spotify Client Secret
REDIRECT_URI = "http://127.0.0.1:5000/redirect"  # Redirect URI configured in Spotify Dashboard



# Authenticate with Spotify
scope = "user-top-read"
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=CLIENT_ID,
                                               client_secret=CLIENT_SECRET,
                                               redirect_uri=REDIRECT_URI,
                                               scope=scope,
                                               cache_path="spotify_token_cache"))


# Fetch Fact Tables (2)
# Fetch Fact Table 1: Top Artists
def get_top_artists(time_range, limit):
    """
    Fetch the user's top artists.
    Args:
        time_range (str): Time range ('short_term', 'medium_term', 'long_term').
        limit (int): Number of top artists to fetch (default: 20).
    Returns:
        DataFrame: A pandas DataFrame with artist names and genres.
    """
    top_artists = sp.current_user_top_artists(time_range=time_range, limit=limit)
    artist_data = []
    for artist in top_artists['items']:
        artist_data.append({
            'artistID': artist['id'],
            'Artist': artist['name'],
            'Genres': ", ".join(artist['genres']),  # Combine multiple genres into a string
            'Popularity': artist['popularity'],  # Spotify popularity score
            'Image URL': artist['images'][0]['url'],
            'time_range': time_range  # Spotify popularity score
        })
    return pd.DataFrame(artist_data)


# Fetch Fact Table 2: Top Artists
def get_top_tracks(time_range, limit):
    """
    Fetch the user's top tracks.
    Args:
        time_range (str): Time range ('short_term', 'medium_term', 'long_term').
        limit (int): Number of top tracks to fetch (default: 20).
    Returns:
        DataFrame: A pandas DataFrame with tracks names.
    """
    top_tracks = sp.current_user_top_tracks(time_range=time_range, limit=limit)
    track_data = []
    for track in top_tracks['items']:
        track_data.append({
            'albumID': track['album']['id'],
            'artistID': track['artists'][0]['id'],
            'trackID': track['id'],
            'Track': track['name'],
            'Albums': track['album']['name'],  # Combine multiple genres into a string
            'Artist(s)': ", ".join(track['name'] for track in track['artists']),
            'Popularity': track['popularity'],  # Spotify popularity score
            'Image URL': track['album']['images'][0]['url'],
            'time_range': time_range  # Spotify popularity score
        })
    return pd.DataFrame(track_data)

# Fetch Dimension Tables (3)
# Fetch Dim Table 1: Artists
def Get_Artist_Dim(artist_ids):
    """
    Fetch the artists and relevant information for dimension table.
    Args:
        id (str): Unique identifier to get artist information.
    Returns:
        DataFrame: A pandas DataFrame with artists and relevant information.
    """
    artists = sp.artists(artist_ids)
    artist_data = []
    for artist in artists['artists']:
        # Handle cases where 'images' might be empty or not a list
        image_url = artist['images'][0]['url'] if artist['images'] else None
        
        artist_data.append({
            'artistID': artist['id'],
            'artistName': artist['name'],  # Combine multiple genres into a string
            'Genres': ", ".join(artist['genres']),  # Combine multiple genres into a string
            'Popularity': artist['popularity'],  # Spotify popularity score
            'Image URL': image_url
        })
    return pd.DataFrame(artist_data)



# Defining main function
def main():
    
    # Initialize final tables for artists and tracks
    final_artists_fact = pd.DataFrame()
    final_tracks_fact = pd.DataFrame()
    
    # Terms for pulling artist and track fact tables
    time_range = ['short_term', 'medium_term', 'long_term']
    
    # For loop to rotate through time range array
    for ranges in time_range:
        # Append data for artists df - Fact
        df_top_artists = get_top_artists(ranges, limit=50)
        
        df_top_artists['Rank'] = df_top_artists.index + 1
        final_artists_fact = pd.concat([final_artists_fact, df_top_artists], ignore_index=True)
        
        # Append data for tracks df - Fact
        df_top_tracks = get_top_tracks(ranges, limit=50)
    
        df_top_tracks['Rank'] = df_top_tracks.index + 1
        final_tracks_fact = pd.concat([final_tracks_fact, df_top_tracks], ignore_index=True)
    
    # Get Dimension data
    
    ## Artist Dimension Creation ##
    # Convert artist column from artists and track fact tables.
    my_track_list = final_tracks_fact['artistID'].tolist()
    my_artist_list = final_artists_fact['artistID'].tolist()
    
    combined_artist_list = my_track_list + my_artist_list
    # combined_artist_list = combined_artist_list[:49]
    
    # Get length of list
    list_length = len(combined_artist_list)
    
    # Define chunk size
    chunk_size = 50
    
    # Create albums dataframe
    albums_dim = pd.DataFrame()
    
    # Use slicing in a loop
    for i in range(0, list_length, chunk_size):
        slice_chunk = combined_artist_list[i:i + chunk_size]
    
        df_artists_dim = Get_Artist_Dim(slice_chunk)
        albums_dim = pd.concat([albums_dim, df_artists_dim], ignore_index=True)
    
    
    ## Save final tables to CSV ##
    
    # Fact Tables
    final_artists_fact.to_csv("top_artists_Fact.csv", index=False)
    final_tracks_fact.to_csv("top_tracks_Fact.csv", index=False)
    
    # Dimension Tables
    albums_dim.to_csv("artists_dim.csv", index=False)
    
    # Successful run
    print("CSV files saved successfully!")



# Using the special variable 
# __name__
if __name__=="__main__":
    main()
    


# In[ ]:




