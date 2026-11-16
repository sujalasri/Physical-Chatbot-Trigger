
import base64
import json
import re
import os
import time
import socket
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

PROJECT_NAME = "Physical Chatbot Trigger"
TABLE_NAME = "PhysicalChatbotLogs"

SNS_TOPIC_ARN = (
    "arn:aws:sns:us-east-1:686055675086:"
    "PhysicalChatbotNotifications"
)

GEMINI_MODEL = "gemini-3.7-flash"

MAX_MESSAGE_LENGTH = 200
MAX_BODY_BYTES = 2048

SOURCE_WEBSITE = "website"
SOURCE_WOKWI = "wokwi-pushbutton"

SOURCE_LABELS = {
    SOURCE_WEBSITE: "Website",
    SOURCE_WOKWI: "Physical pushbutton (Wokwi)"
}

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)
sns = boto3.client("sns")


def http_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS"
        },
        "body": json.dumps(body, default=str)
    }


def clean_text(value):
    if not isinstance(value, str):
        return ""

    return re.sub(
        r"[\x00-\x1f\x7f]",
        " ",
        value
    ).strip()


def build_response(message):
    """Generate an answer with Gemini, retrying temporary service failures."""
    api_key = os.environ.get("GEMINI_API_KEY")

    if not api_key:
        raise Exception("GEMINI_API_KEY is missing")

    url = (
        "https://generativelanguage.googleapis.com/"
        "v1beta/models/"
        + GEMINI_MODEL
        + ":generateContent"
    )

    payload = {
        "systemInstruction": {
            "parts": [{
                "text": (
                    "You are an intelligent Physical Assistance Chatbot. "
                    "Answer user questions clearly, accurately and helpfully. "
                    "Understand the question and provide relevant answers. "
                    "Use simple language. For emergencies, advise contacting "
                    "local emergency services immediately. For medical "
                    "situations, provide general safety guidance and never "
                    "claim to replace a qualified medical professional."
                )
            }]
        },
        "contents": [{
            "role": "user",
            "parts": [{"text": message}]
        }],
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": 512
        }
    }

    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": api_key
        },
        method="POST"
    )

    max_attempts = 3
    retryable_statuses = {429, 500, 502, 503, 504}

    for attempt in range(1, max_attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=12) as response:
                result = json.loads(response.read().decode("utf-8"))

            candidates = result.get("candidates", [])

            if not candidates:
                raise Exception("Gemini returned no answer")

            parts = (
                candidates[0]
                .get("content", {})
                .get("parts", [])
            )

            answer = " ".join(
                part.get("text", "")
                for part in parts
                if part.get("text")
            ).strip()

            if not answer:
                raise Exception("Gemini returned an empty answer")

            return answer

        except urllib.error.HTTPError as exc:
            status = exc.code

            try:
                error_body = exc.read().decode(
                    "utf-8",
                    errors="replace"
                )
            finally:
                exc.close()

            print(
                "Gemini HTTP error:",
                status,
                error_body[:1000]
            )

            if status not in retryable_statuses or attempt == max_attempts:
                raise Exception(
                    f"Gemini API request failed with HTTP {status}"
                )

            wait_seconds = 2 ** (attempt - 1)

            print(
                f"Temporary Gemini HTTP {status}; "
                f"retrying in {wait_seconds} second(s) "
                f"(attempt {attempt + 1}/{max_attempts})"
            )

            time.sleep(wait_seconds)

        except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
            print(
                f"Gemini connection/timeout error on attempt "
                f"{attempt}/{max_attempts}: {type(exc).__name__}"
            )

            if attempt == max_attempts:
                raise Exception(
                    "Gemini API did not respond after 3 attempts"
                )

            wait_seconds = 2 ** (attempt - 1)

            print(
                f"Retrying Gemini request in {wait_seconds} second(s)"
            )

            time.sleep(wait_seconds)


def build_iot_response(message):

    if "button pressed" in message.lower():
        return (
            "Physical assistance alert received. "
            "Your notification has been processed."
        )

    return "Physical chatbot received your message."


def process_event(
    event_id,
    source,
    event_type,
    message,
    notify
):

    timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    ai_status = "NOT_REQUIRED"

    if source == SOURCE_WEBSITE:

        try:
            reply = build_response(message)
            ai_status = "COMPLETED"

        except Exception as exc:
            print("AI generation error:", str(exc))

            reply = (
                "Sorry, I could not generate an answer "
                "right now. Please try again."
            )

            ai_status = "FAILED"

    else:
        reply = build_iot_response(message)

    item = {
        "eventId": event_id,
        "timestamp": timestamp,
        "triggerSource": source,
        "eventType": event_type,
        "message": message,
        "response": reply,
        "aiStatus": ai_status,
        "processingStatus": "PROCESSING",
        "notificationStatus": "PENDING"
    }

    try:
        table.put_item(
            Item=item,
            ConditionExpression=(
                "attribute_not_exists(eventId)"
            )
        )

    except ClientError as exc:

        code = exc.response.get(
            "Error", {}
        ).get("Code", "")

        if code == "ConditionalCheckFailedException":
            return 409, {
                "error": "This event was already processed",
                "eventId": event_id
            }

        print("DynamoDB error:", code)

        return 500, {
            "error": "Could not save the message"
        }

    except Exception as exc:

        print(
            "DynamoDB error:",
            type(exc).__name__
        )

        return 500, {
            "error": "Could not save the message"
        }

    notification = "NOT_REQUIRED"
    sns_message_id = None
    error_detail = None

    if notify:

        try:
            result = sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"{PROJECT_NAME}: New message",
                Message=(
                    f"Project: {PROJECT_NAME}\n"
                    f"Event ID: {event_id}\n"
                    f"Source: {SOURCE_LABELS[source]}\n"
                    f"Time (UTC): {timestamp}\n"
                    f"Message received: {message}\n"
                    f"Chatbot response: {reply}\n"
                    f"AI status: {ai_status}\n"
                )
            )

            notification = "ACCEPTED_BY_SNS"

            sns_message_id = result.get(
                "MessageId"
            )

        except Exception as exc:

            notification = "FAILED"

            error_detail = (
                f"SNS publish failed: "
                f"{type(exc).__name__}"
            )

            print("SNS error:", repr(exc))

    if ai_status == "FAILED":
        processing = "AI_FAILED"

    elif notification == "FAILED":
        processing = "NOTIFICATION_FAILED"

    else:
        processing = "COMPLETED"

    try:

        update_expression = (
            "SET processingStatus = :p, "
            "notificationStatus = :n, "
            "aiStatus = :a"
        )

        values = {
            ":p": processing,
            ":n": notification,
            ":a": ai_status
        }

        if sns_message_id:

            update_expression += ", snsMessageId = :m"

            values[":m"] = sns_message_id

        if error_detail:

            update_expression += ", errorDetail = :e"

            values[":e"] = error_detail

        table.update_item(
            Key={"eventId": event_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=values
        )

    except Exception as exc:

        print(
            "DynamoDB update error:",
            type(exc).__name__
        )

        return 500, {
            "error": "Final status update failed",
            "eventId": event_id,
            "notificationStatus": notification
        }

    status_code = (
        502
        if processing != "COMPLETED"
        else 200
    )

    return status_code, {
        "eventId": event_id,
        "timestamp": timestamp,
        "triggerSource": source,
        "eventType": event_type,
        "message": reply,
        "aiStatus": ai_status,
        "processingStatus": processing,
        "notificationStatus": notification,
        "snsMessageId": sns_message_id,
        "error": error_detail
    }


def handle_website(event, context):

    raw = event.get("body") or "{}"

    try:

        if event.get("isBase64Encoded"):
            raw = base64.b64decode(
                raw
            ).decode("utf-8")

        if len(raw.encode("utf-8")) > MAX_BODY_BYTES:
            return http_response(
                413,
                {"error": "Request is too large"}
            )

        body = json.loads(raw)

    except Exception:

        return http_response(
            400,
            {"error": "Invalid JSON request"}
        )

    if not isinstance(body, dict):

        return http_response(
            400,
            {"error": "Invalid request format"}
        )

    message = body.get("message", "")

    if not isinstance(message, str):

        return http_response(
            400,
            {"error": "Message must be text"}
        )

    message = clean_text(message)

    if not message:

        return http_response(
            400,
            {"error": "Please enter a message"}
        )

    if len(message) > MAX_MESSAGE_LENGTH:

        return http_response(
            400,
            {
                "error": (
                    "Message must be at most "
                    "200 characters"
                )
            }
        )

    event_id = context.aws_request_id

    status, result = process_event(
        event_id,
        SOURCE_WEBSITE,
        "TRIGGER",
        message,
        True
    )

    return http_response(status, result)


def handle_iot(event, context):

    print("IoT trigger received")

    raw = event.get(
        "message",
        "BUTTON PRESSED!"
    )

    message = clean_text(raw)[:MAX_MESSAGE_LENGTH]

    if not message:
        message = "BUTTON PRESSED!"

    is_press = (
        "BUTTON PRESSED" in message.upper()
    )

    status, result = process_event(
        context.aws_request_id,
        SOURCE_WOKWI,
        "TRIGGER" if is_press else "MESSAGE",
        message,
        is_press
    )

    return {
        "statusCode": status,
        "body": json.dumps(result, default=str)
    }


def lambda_handler(event, context):

    if isinstance(event, dict) and "requestContext" in event:

        http = (
            event.get("requestContext") or {}
        ).get("http") or {}

        method = http.get("method", "")

        path = (
            http.get("path") or "/"
        ).rstrip("/") or "/"

        print(
            f"Website request: {method} {path}"
        )

        if method == "OPTIONS":

            return http_response(
                200,
                {"message": "CORS OK"}
            )

        if method == "POST" and path in (
            "/chat",
            "/",
            "/events"
        ):

            return handle_website(
                event,
                context
            )

        return http_response(
            404,
            {"error": "Route not found"}
        )

    return handle_iot(event, context)
