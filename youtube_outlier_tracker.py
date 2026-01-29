"""
YouTube Competitor Outlier Tracker
==================================
Checks competitor channels daily for viral/outlier videos and logs them to Notion.

Author: Built for Gld
"""

import os
import json
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import requests

# =============================================================================
# CONFIGURATION
# =============================================================================

YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
NOTION_API_KEY = os.environ.get("NOTION_API_KEY")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")

# Outlier threshold: videos with X times the channel average are flagged
OUTLIER_THRESHOLD = 1.5  # 1.5x channel average = outlier (adjust as needed)

# How many recent videos to use for calculating channel average
VIDEOS_FOR_AVERAGE = 20

# How far back to look for new uploads (in hours)
LOOKBACK_HOURS = 168  # 7 days - extended for niche topics

# Topic filter: only include videos with these keywords in the title (case-insensitive)
# Set to empty list [] to include all topics
TOPIC_KEYWORDS = [
    "volcano",
    "volcanic",
    "eruption",
    "erupts",
    "erupting",
    "lava",
    "magma",
    "caldera",
    "krakatoa",
    "vesuvius",
    "yellowstone",
    "supervolcano",
    "pompeii",
    "ash cloud",
    "pyroclastic"
]

# =============================================================================
# LOAD COMPETITOR CHANNELS
# =============================================================================

def load_channels():
    """Load competitor channel IDs from config file."""
    config_path = os.path.join(os.path.dirname(__file__), "channels.json")
    
    if not os.path.exists(config_path):
        print("ERROR: channels.json not found. Please create it with your competitor channels.")
        return []
    
    with open(config_path, "r") as f:
        data = json.load(f)
    
    return data.get("channels", [])

# =============================================================================
# YOUTUBE API FUNCTIONS
# =============================================================================

def get_youtube_client():
    """Initialize YouTube API client."""
    if not YOUTUBE_API_KEY:
        raise ValueError("YOUTUBE_API_KEY environment variable not set")
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

def get_channel_info(youtube, channel_identifier):
    """Get channel name and upload playlist ID. Supports both channel IDs and handles."""
    try:
        # Check if it's a handle (@username) or channel ID (UCxxxxx)
        if channel_identifier.startswith("@"):
            # It's a handle - use forHandle parameter
            response = youtube.channels().list(
                part="snippet,contentDetails",
                forHandle=channel_identifier[1:]  # Remove @ prefix
            ).execute()
        elif channel_identifier.startswith("UC"):
            # It's a channel ID
            response = youtube.channels().list(
                part="snippet,contentDetails",
                id=channel_identifier
            ).execute()
        else:
            # Assume it's a handle without @
            response = youtube.channels().list(
                part="snippet,contentDetails",
                forHandle=channel_identifier
            ).execute()
        
        if not response.get("items"):
            print(f"  WARNING: Channel {channel_identifier} not found")
            return None
        
        channel = response["items"][0]
        return {
            "id": channel["id"],
            "name": channel["snippet"]["title"],
            "uploads_playlist": channel["contentDetails"]["relatedPlaylists"]["uploads"]
        }
    except HttpError as e:
        print(f"  ERROR fetching channel {channel_identifier}: {e}")
        return None

def get_recent_videos(youtube, uploads_playlist_id, max_results=50):
    """Get recent video IDs from a channel's uploads playlist."""
    try:
        response = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=max_results
        ).execute()
        
        video_ids = [item["contentDetails"]["videoId"] for item in response.get("items", [])]
        return video_ids
    except HttpError as e:
        print(f"  ERROR fetching playlist: {e}")
        return []

def get_video_details(youtube, video_ids):
    """Get detailed stats for a list of video IDs."""
    if not video_ids:
        return []
    
    videos = []
    
    # YouTube API allows max 50 IDs per request
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            response = youtube.videos().list(
                part="snippet,statistics",
                id=",".join(batch)
            ).execute()
            
            for item in response.get("items", []):
                stats = item.get("statistics", {})
                snippet = item.get("snippet", {})
                
                # Parse publish date
                published_str = snippet.get("publishedAt", "")
                try:
                    published_at = datetime.fromisoformat(published_str.replace("Z", "+00:00"))
                except:
                    published_at = None
                
                videos.append({
                    "id": item["id"],
                    "title": snippet.get("title", "Unknown"),
                    "published_at": published_at,
                    "views": int(stats.get("viewCount", 0)),
                    "likes": int(stats.get("likeCount", 0)),
                    "comments": int(stats.get("commentCount", 0)),
                    "url": f"https://www.youtube.com/watch?v={item['id']}",
                    "thumbnail": snippet.get("thumbnails", {}).get("high", {}).get("url", "")
                })
        except HttpError as e:
            print(f"  ERROR fetching video details: {e}")
    
    return videos

def calculate_channel_average(videos):
    """Calculate average views from a list of videos."""
    if not videos:
        return 0
    
    total_views = sum(v["views"] for v in videos)
    return total_views / len(videos)

def matches_topic_filter(title):
    """Check if video title matches any topic keywords."""
    if not TOPIC_KEYWORDS:
        return True  # No filter, include all
    
    title_lower = title.lower()
    return any(keyword.lower() in title_lower for keyword in TOPIC_KEYWORDS)

def find_outliers(channel_name, videos, channel_average, lookback_hours=24):
    """Find videos that are outliers (above threshold) and uploaded recently."""
    now = datetime.now(timezone.utc)
    cutoff_time = now - timedelta(hours=lookback_hours)
    
    outliers = []
    
    for video in videos:
        # Skip if no publish date
        if not video["published_at"]:
            continue
        
        # Check if video is within lookback window
        if video["published_at"] < cutoff_time:
            continue
        
        # Calculate outlier score
        if channel_average > 0:
            outlier_score = video["views"] / channel_average
        else:
            outlier_score = 0
        
        # Calculate views per hour (velocity)
        hours_since_upload = max((now - video["published_at"]).total_seconds() / 3600, 1)
        views_per_hour = video["views"] / hours_since_upload
        
        # Check topic filter - if keywords set, only include matching videos
        if not matches_topic_filter(video["title"]):
            continue
        
        # Add all matching videos (no threshold when using topic filter)
        if TOPIC_KEYWORDS or outlier_score >= OUTLIER_THRESHOLD:
            outliers.append({
                **video,
                "channel_name": channel_name,
                "channel_average": round(channel_average),
                "outlier_score": round(outlier_score, 2),
                "views_per_hour": round(views_per_hour),
                "hours_since_upload": round(hours_since_upload, 1)
            })
    
    return outliers

# =============================================================================
# NOTION API FUNCTIONS
# =============================================================================

def delete_old_entries(days_to_keep=7):
    """Delete entries older than X days from Notion database."""
    if not NOTION_API_KEY or not NOTION_DATABASE_ID:
        return
    
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Calculate cutoff date
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days_to_keep)).strftime("%Y-%m-%d")
    
    print(f"\n🧹 Cleaning up entries older than {days_to_keep} days (before {cutoff_date})...")
    
    # Query for old entries
    query_payload = {
        "filter": {
            "property": "Found Date",
            "date": {
                "before": cutoff_date
            }
        }
    }
    
    try:
        response = requests.post(
            f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
            headers=headers,
            json=query_payload
        )
        
        if response.status_code != 200:
            print(f"  WARNING: Could not query old entries: {response.text}")
            return
        
        results = response.json().get("results", [])
        
        if not results:
            print("  No old entries to delete")
            return
        
        print(f"  Found {len(results)} old entries to delete...")
        
        # Archive (delete) each old entry
        deleted_count = 0
        for page in results:
            page_id = page["id"]
            
            archive_response = requests.patch(
                f"https://api.notion.com/v1/pages/{page_id}",
                headers=headers,
                json={"archived": True}
            )
            
            if archive_response.status_code == 200:
                deleted_count += 1
            else:
                print(f"  WARNING: Failed to delete entry: {archive_response.text}")
        
        print(f"  ✅ Deleted {deleted_count} old entries")
        
    except Exception as e:
        print(f"  ERROR cleaning up old entries: {e}")

def get_existing_video_urls():
    """Get all video URLs already in the Notion database to prevent duplicates."""
    if not NOTION_API_KEY or not NOTION_DATABASE_ID:
        return set()
    
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    existing_urls = set()
    has_more = True
    start_cursor = None
    
    while has_more:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor
        
        try:
            response = requests.post(
                f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
                headers=headers,
                json=payload
            )
            
            if response.status_code != 200:
                break
            
            data = response.json()
            
            for page in data.get("results", []):
                url_prop = page.get("properties", {}).get("URL", {})
                if url_prop.get("url"):
                    existing_urls.add(url_prop["url"])
            
            has_more = data.get("has_more", False)
            start_cursor = data.get("next_cursor")
            
        except Exception as e:
            print(f"  WARNING: Could not fetch existing URLs: {e}")
            break
    
    return existing_urls

def send_to_notion(outliers):
    """Send outlier videos to Notion database."""
    if not NOTION_API_KEY or not NOTION_DATABASE_ID:
        print("WARNING: Notion credentials not set. Skipping Notion sync.")
        return False
    
    # Get existing URLs to prevent duplicates
    print("  Checking for duplicates...")
    existing_urls = get_existing_video_urls()
    print(f"  Found {len(existing_urls)} existing entries in database")
    
    # Filter out duplicates
    new_outliers = [v for v in outliers if v["url"] not in existing_urls]
    skipped = len(outliers) - len(new_outliers)
    
    if skipped > 0:
        print(f"  Skipping {skipped} videos already in database")
    
    if not new_outliers:
        print("  No new videos to add")
        return True
    
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    success_count = 0
    
    for video in new_outliers:
        # Build Notion page properties
        properties = {
            "Title": {
                "title": [{"text": {"content": video["title"][:100]}}]
            },
            "Channel": {
                "select": {"name": video["channel_name"][:100]}
            },
            "Views": {
                "number": video["views"]
            },
            "Outlier Score": {
                "number": video["outlier_score"]
            },
            "Channel Average": {
                "number": video["channel_average"]
            },
            "Views/Hour": {
                "number": video["views_per_hour"]
            },
            "URL": {
                "url": video["url"]
            },
            "Published": {
                "date": {"start": video["published_at"].isoformat()}
            },
            "Found Date": {
                "date": {"start": datetime.now(timezone.utc).isoformat()}
            }
        }
        
        payload = {
            "parent": {"database_id": NOTION_DATABASE_ID},
            "properties": properties
        }
        
        # Add thumbnail as cover if available
        if video.get("thumbnail"):
            payload["cover"] = {
                "type": "external",
                "external": {"url": video["thumbnail"]}
            }
        
        try:
            response = requests.post(
                "https://api.notion.com/v1/pages",
                headers=headers,
                json=payload
            )
            
            if response.status_code == 200:
                success_count += 1
            else:
                print(f"  WARNING: Failed to add '{video['title'][:50]}...' to Notion: {response.text}")
        except Exception as e:
            print(f"  ERROR adding to Notion: {e}")
    
    print(f"  Added {success_count}/{len(new_outliers)} new videos to Notion")
    return success_count > 0

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    print("=" * 60)
    print("YOUTUBE COMPETITOR OUTLIER TRACKER")
    print(f"Run time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)
    
    # Clean up old entries first (older than 7 days)
    delete_old_entries(days_to_keep=7)
    
    # Load channels
    channels_config = load_channels()
    if not channels_config:
        print("No channels configured. Add channels to channels.json")
        return
    
    print(f"\nTracking {len(channels_config)} competitor channels...")
    print(f"Looking for videos from the last {LOOKBACK_HOURS} hours ({LOOKBACK_HOURS // 24} days)")
    if TOPIC_KEYWORDS:
        print(f"Topic filter: {', '.join(TOPIC_KEYWORDS[:5])}{'...' if len(TOPIC_KEYWORDS) > 5 else ''}")
        print("Mode: Capturing ALL matching videos (no threshold)")
    else:
        print(f"Outlier threshold: {OUTLIER_THRESHOLD}x channel average")
    print()
    
    # Initialize YouTube client
    try:
        youtube = get_youtube_client()
    except ValueError as e:
        print(f"ERROR: {e}")
        return
    
    all_outliers = []
    
    # Process each channel
    for channel_entry in channels_config:
        channel_id = channel_entry.get("id") or channel_entry  # Support both formats
        channel_label = channel_entry.get("name", channel_id) if isinstance(channel_entry, dict) else channel_id
        
        print(f"\n📺 Processing: {channel_label}")
        
        # Get channel info
        channel_info = get_channel_info(youtube, channel_id)
        if not channel_info:
            continue
        
        channel_name = channel_info["name"]
        print(f"   Channel: {channel_name}")
        
        # Get recent videos
        video_ids = get_recent_videos(youtube, channel_info["uploads_playlist"], VIDEOS_FOR_AVERAGE)
        if not video_ids:
            print("   No videos found")
            continue
        
        # Get video details
        videos = get_video_details(youtube, video_ids)
        print(f"   Found {len(videos)} recent videos")
        
        # Calculate channel average
        channel_average = calculate_channel_average(videos)
        print(f"   Channel average: {channel_average:,.0f} views")
        
        # Find outliers
        outliers = find_outliers(channel_name, videos, channel_average, LOOKBACK_HOURS)
        
        if outliers:
            print(f"   🔥 Found {len(outliers)} potential outliers!")
            for o in outliers:
                print(f"      - {o['title'][:50]}... ({o['views']:,} views, {o['outlier_score']}x avg)")
            all_outliers.extend(outliers)
        else:
            print("   No outliers in the last 24 hours")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    if all_outliers:
        # Sort by outlier score
        all_outliers.sort(key=lambda x: x["outlier_score"], reverse=True)
        
        print(f"\n🔥 Total outliers found: {len(all_outliers)}\n")
        print("TOP PERFORMERS:")
        print("-" * 40)
        
        for i, video in enumerate(all_outliers[:10], 1):
            print(f"{i}. [{video['channel_name']}] {video['title'][:40]}...")
            print(f"   Views: {video['views']:,} | Score: {video['outlier_score']}x | {video['views_per_hour']:,}/hr")
            print(f"   {video['url']}")
            print()
        
        # Send to Notion
        print("\nSyncing to Notion...")
        send_to_notion(all_outliers)
    else:
        print("\nNo outliers found across all channels in the last 24 hours.")
        print("This could mean:")
        print("  - No competitors uploaded in this window")
        print("  - No videos hit the outlier threshold yet")
        print("  - Try lowering OUTLIER_THRESHOLD in the script")
    
    print("\n✅ Done!")

if __name__ == "__main__":
    main()
