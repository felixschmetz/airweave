"""Google Calendar-specific bongo implementation."""

import asyncio
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List

import httpx
from monke.bongos.base_bongo import BaseBongo
from monke.utils.logging import get_logger


class GoogleCalendarBongo(BaseBongo):
    """Google Calendar-specific bongo implementation.
    
    Creates, updates, and deletes test events via the real Google Calendar API.
    """

    connector_type = "google_calendar"

    def __init__(self, credentials: Dict[str, Any], **kwargs):
        """Initialize the Google Calendar bongo.
        
        Args:
            credentials: Google Calendar credentials with access_token
            **kwargs: Additional configuration (e.g., entity_count)
        """
        super().__init__(credentials)
        self.access_token = credentials["access_token"]
        
        # Configuration from kwargs
        self.entity_count = kwargs.get('entity_count', 10)
        self.openai_model = kwargs.get('openai_model', 'gpt-5')
        
        # Test data tracking
        self.test_events = []
        self.test_calendar_id = None
        
        # Rate limiting (Google Calendar: 500 requests per 100 seconds)
        self.last_request_time = 0
        self.rate_limit_delay = 0.5  # 0.5 second between requests (conservative)
        
        # Logger
        self.logger = get_logger("google_calendar_bongo")
    
    async def create_entities(self) -> List[Dict[str, Any]]:
        """Create test events in Google Calendar."""
        self.logger.info(f"🥁 Creating {self.entity_count} test events in Google Calendar")
        entities = []
        
        # First, create or get a test calendar
        await self._ensure_test_calendar()
        
        # Create events based on configuration
        from monke.generation.google_calendar import generate_google_calendar_artifact
        
        for i in range(self.entity_count):
            # Short unique token used in title and description for verification
            token = str(uuid.uuid4())[:8]
            
            title, description, duration_hours = await generate_google_calendar_artifact(self.openai_model, token)
            
            # Create event with future start time
            start_time = datetime.utcnow() + timedelta(days=i+1)
            end_time = start_time + timedelta(hours=duration_hours)
            
            event_data = await self._create_test_event(
                self.test_calendar_id,
                title,
                description,
                start_time,
                end_time
            )
            
            entities.append({
                "type": "event",
                "id": event_data["id"],
                "calendar_id": self.test_calendar_id,
                "title": title,
                "token": token,
                "expected_content": token,
            })
            
            self.logger.info(f"📅 Created test event: {event_data['id']}")
            
            # Rate limiting
            if self.entity_count > 10:
                await asyncio.sleep(0.5)
        
        self.test_events = entities  # Store for later operations
        return entities
    
    async def update_entities(self) -> List[Dict[str, Any]]:
        """Update test entities in Google Calendar."""
        self.logger.info("🥁 Updating test events in Google Calendar")
        updated_entities = []
        
        # Update a subset of events based on configuration
        from monke.generation.google_calendar import generate_google_calendar_artifact
        events_to_update = min(3, self.entity_count)  # Update max 3 events for any test size
        
        for i in range(events_to_update):
            if i < len(self.test_events):
                event_info = self.test_events[i]
                token = event_info.get("token") or str(uuid.uuid4())[:8]
                
                # Generate new content with same token
                title, description, duration_hours = await generate_google_calendar_artifact(
                    self.openai_model, token, is_update=True
                )
                
                # Update event time (move it 1 hour later)
                await self._update_test_event(
                    self.test_calendar_id,
                    event_info["id"],
                    title,
                    description
                )
                
                updated_entities.append({
                    "type": "event",
                    "id": event_info["id"],
                    "calendar_id": self.test_calendar_id,
                    "title": title,
                    "token": token,
                    "expected_content": token,
                    "updated": True,
                })
                
                self.logger.info(f"📝 Updated test event: {event_info['id']}")
                
                # Rate limiting
                if self.entity_count > 10:
                    await asyncio.sleep(0.5)
        
        return updated_entities
    
    async def delete_entities(self) -> List[str]:
        """Delete all test entities from Google Calendar."""
        self.logger.info("🥁 Deleting all test events from Google Calendar")
        
        # Use the specific deletion method to delete all entities
        return await self.delete_specific_entities(self.created_entities)
    
    async def delete_specific_entities(self, entities: List[Dict[str, Any]]) -> List[str]:
        """Delete specific entities from Google Calendar."""
        self.logger.info(f"🥁 Deleting {len(entities)} specific events from Google Calendar")
        
        deleted_ids = []
        
        for entity in entities:
            try:
                # Find the corresponding test event
                test_event = next((te for te in self.test_events if te["id"] == entity["id"]), None)
                
                if test_event:
                    await self._delete_test_event(self.test_calendar_id, test_event["id"])
                    deleted_ids.append(test_event["id"])
                    self.logger.info(f"🗑️ Deleted test event: {test_event['id']}")
                else:
                    self.logger.warning(f"⚠️ Could not find test event for entity: {entity.get('id')}")
                
                # Rate limiting
                if len(entities) > 10:
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Could not delete entity {entity.get('id')}: {e}")
        
        # VERIFICATION: Check if events are actually deleted
        self.logger.info("🔍 VERIFYING: Checking if events are actually deleted from Google Calendar")
        for entity in entities:
            if entity["id"] in deleted_ids:
                is_deleted = await self._verify_event_deleted(self.test_calendar_id, entity["id"])
                if is_deleted:
                    self.logger.info(f"✅ Event {entity['id']} confirmed deleted from Google Calendar")
                else:
                    self.logger.warning(f"⚠️ Event {entity['id']} still exists in Google Calendar!")
        
        return deleted_ids
    
    async def cleanup(self):
        """Clean up any remaining test data."""
        self.logger.info("🧹 Cleaning up remaining test events in Google Calendar")
        
        # Force delete any remaining test events
        for test_event in self.test_events:
            try:
                await self._force_delete_event(self.test_calendar_id, test_event["id"])
                self.logger.info(f"🧹 Force deleted event: {test_event['id']}")
            except Exception as e:
                self.logger.warning(f"⚠️ Could not force delete event {test_event['id']}: {e}")
        
        # Delete the test calendar if it was created
        if self.test_calendar_id and self.test_calendar_id != "primary":
            try:
                await self._delete_test_calendar(self.test_calendar_id)
                self.logger.info(f"🧹 Deleted test calendar: {self.test_calendar_id}")
            except Exception as e:
                self.logger.warning(f"⚠️ Could not delete test calendar: {e}")
    
    # Helper methods for Google Calendar API calls
    async def _ensure_test_calendar(self):
        """Ensure we have a test calendar to work with."""
        await self._rate_limit()
        
        # Create a new test calendar
        calendar_name = f"Monke Test Calendar - {str(uuid.uuid4())[:8]}"
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://www.googleapis.com/calendar/v3/calendars",
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                },
                json={
                    "summary": calendar_name,
                    "description": "Temporary calendar for Monke testing",
                    "timeZone": "UTC"
                }
            )
            
            if response.status_code != 200:
                # If we can't create a calendar, use primary
                self.logger.warning("Could not create test calendar, using primary calendar")
                self.test_calendar_id = "primary"
            else:
                result = response.json()
                self.test_calendar_id = result["id"]
                self.logger.info(f"📅 Created test calendar: {self.test_calendar_id}")
    
    async def _create_test_event(
        self,
        calendar_id: str,
        title: str,
        description: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """Create a test event via Google Calendar API."""
        await self._rate_limit()
        
        event_data = {
            "summary": title,
            "description": description,
            "start": {
                "dateTime": start_time.isoformat() + "Z",
                "timeZone": "UTC"
            },
            "end": {
                "dateTime": end_time.isoformat() + "Z",
                "timeZone": "UTC"
            }
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events",
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                },
                json=event_data
            )
            
            if response.status_code != 200:
                raise Exception(f"Failed to create event: {response.status_code} - {response.text}")
            
            result = response.json()
            
            # Track created event
            self.created_entities.append({
                "id": result["id"],
                "calendar_id": calendar_id
            })
            
            return result
    
    async def _update_test_event(
        self,
        calendar_id: str,
        event_id: str,
        title: str,
        description: str
    ) -> Dict[str, Any]:
        """Update a test event via Google Calendar API."""
        await self._rate_limit()
        
        # First get the existing event
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events/{event_id}",
                headers={
                    "Authorization": f"Bearer {self.access_token}"
                }
            )
            
            if response.status_code != 200:
                raise Exception(f"Failed to get event: {response.status_code} - {response.text}")
            
            event_data = response.json()
            
            # Update title and description
            event_data["summary"] = title
            event_data["description"] = description
            
            # Update the event
            response = await client.put(
                f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events/{event_id}",
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                },
                json=event_data
            )
            
            if response.status_code != 200:
                raise Exception(f"Failed to update event: {response.status_code} - {response.text}")
            
            return response.json()
    
    async def _delete_test_event(self, calendar_id: str, event_id: str):
        """Delete a test event via Google Calendar API."""
        await self._rate_limit()
        
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events/{event_id}",
                headers={
                    "Authorization": f"Bearer {self.access_token}"
                }
            )
            
            if response.status_code not in [204, 410]:  # 204 = success, 410 = already deleted
                raise Exception(f"Failed to delete event: {response.status_code} - {response.text}")
    
    async def _verify_event_deleted(self, calendar_id: str, event_id: str) -> bool:
        """Verify if an event is actually deleted from Google Calendar."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events/{event_id}",
                    headers={
                        "Authorization": f"Bearer {self.access_token}"
                    }
                )
                
                if response.status_code == 404 or response.status_code == 410:
                    # Event not found or deleted - success
                    return True
                elif response.status_code == 200:
                    # Event still exists
                    data = response.json()
                    # Check if event is cancelled
                    return data.get("status") == "cancelled"
                else:
                    # Unexpected response
                    self.logger.warning(f"⚠️ Unexpected response checking {event_id}: {response.status_code}")
                    return False
                    
        except Exception as e:
            self.logger.warning(f"⚠️ Error verifying event deletion for {event_id}: {e}")
            return False
    
    async def _force_delete_event(self, calendar_id: str, event_id: str):
        """Force delete an event."""
        try:
            await self._delete_test_event(calendar_id, event_id)
        except Exception as e:
            self.logger.warning(f"Could not force delete {event_id}: {e}")
    
    async def _delete_test_calendar(self, calendar_id: str):
        """Delete the test calendar."""
        await self._rate_limit()
        
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}",
                headers={
                    "Authorization": f"Bearer {self.access_token}"
                }
            )
            
            if response.status_code not in [204, 404]:
                raise Exception(f"Failed to delete calendar: {response.status_code} - {response.text}")
    
    async def _rate_limit(self):
        """Implement rate limiting for Google Calendar API."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            await asyncio.sleep(sleep_time)
        
        self.last_request_time = time.time()
