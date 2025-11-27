import pytest

from slurm_monitor.db.v2.message_subscriber import MessageSubscriber

@pytest.mark.parametrize("txt,expected_topic,expected_lower_bound,expected_upper_bound",
    [
        ["topic-any","topic-any", None,None],
        ["topic-123","topic-123", None,None],
        ["node.sample:11","node.sample", 11,None],
        ["node.sample:21-55","node.sample", 21,55],
        ["node.sample:-55", None, None, None],
    ])
def test_MessageSubscriber_extract_offset_bounds(txt,
            expected_topic,
            expected_lower_bound,
            expected_upper_bound):

    topic_bound = None
    if expected_topic is None:
        with pytest.raises(ValueError):
            MessageSubscriber.extract_offset_bounds(txt)
    else:
        topic_bound = MessageSubscriber.extract_offset_bounds(txt)

        assert topic_bound.topic == expected_topic
        assert topic_bound.lower_bound == expected_lower_bound
        assert topic_bound.upper_bound == expected_upper_bound

