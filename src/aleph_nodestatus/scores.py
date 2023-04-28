from aleph_nodestatus.messages import process_message_history


def process_scores_history(settings):

    return process_message_history(
        tags=[settings.filter_tag],
        content_types=[settings.scores_post_type],
        api_server=settings.aleph_api_server,
        # min_height=...,
        # request_count=...,
        message_type="POST",
        request_sort='1',
        yield_unconfirmed=False,
        addresses=settings.scores_senders,
        crawl_history=True,
        request_count=10,
    )
