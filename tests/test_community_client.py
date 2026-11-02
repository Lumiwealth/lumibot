import pytest
from unittest.mock import Mock, patch
from lumibot.components.community import CommunityClient
from uuid import uuid4


def test_caller_can_reuse_a_durable_request_id_after_an_uncertain_response():
    response = Mock()
    response.json.return_value = {'post': {'id': 'persisted-post'}}
    identity = str(uuid4())
    client = CommunityClient(api_key='synthetic-community-key')
    with patch('lumibot.components.community.requests.post', side_effect=[TimeoutError, response]) as post:
        with pytest.raises(TimeoutError):
            client.post('A dated public observation.', request_id=identity)
        # A fresh client can explicitly repeat the same operation. The SDK
        # never retries or silently invents a new identity on transport failure.
        recovered = CommunityClient(api_key='synthetic-community-key').post(
            'A dated public observation.', request_id=identity)
        assert recovered['id'] == 'persisted-post'
        assert post.call_count == 2
        assert [call.kwargs['json']['requestId'] for call in post.call_args_list] == [identity, identity]


def test_malformed_request_identity_is_rejected_before_transport():
    with patch('lumibot.components.community.requests.post') as post:
        with pytest.raises(ValueError, match='request_id'):
            CommunityClient(api_key='synthetic-key').post('Public research.', request_id='not-a-uuid')
        post.assert_not_called()


def test_post_uses_scoped_key_and_server_verification_without_retry():
    response=Mock()
    response.json.return_value={'post': {'id': 'post-1'}}
    client=CommunityClient(api_key='test-community-key')
    with patch('lumibot.components.community.requests.post', return_value=response) as post:
        assert client.post('Explain a dated market observation.')['id'] == 'post-1'
        assert post.call_count == 1
        assert post.call_args.kwargs['timeout'] == 15
        assert post.call_args.kwargs['headers']['Authorization'] == 'Bearer test-community-key'
        assert 'sharedTrade' not in post.call_args.kwargs['json']


def test_post_limit_is_checked_without_sending_or_truncating():
    client=CommunityClient(api_key='test-community-key')
    with patch('lumibot.components.community.requests.post') as post:
        with pytest.raises(ValueError): client.post('x'*100001)
        post.assert_not_called()


def test_transport_error_is_visible_and_never_retries():
    client=CommunityClient(api_key='test-community-key')
    with patch('lumibot.components.community.requests.post', side_effect=TimeoutError) as post:
        with pytest.raises(TimeoutError): client.post('Share this observation.')
        assert post.call_count == 1
