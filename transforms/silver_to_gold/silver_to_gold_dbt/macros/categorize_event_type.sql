{% macro categorize_event_type(event_type) %}
  CASE
    WHEN event_type IN ('PushEvent', 'CreateEvent', 'ReleaseEvent') THEN 'Code'
    WHEN event_type IN ('PullRequestEvent', 'PullRequestReviewCommentEvent') THEN 'PR'
    WHEN event_type IN ('IssuesEvent', 'IssueCommentEvent') THEN 'Issue'
    ELSE 'Other'
  END
{% endmacro %}

