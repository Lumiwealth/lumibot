import pytest
from unittest.mock import Mock, patch
from lumibot.components.community import CommunityClient


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


@pytest.mark.parametrize('parent_post_id', [None, 'root-post'])
def test_caller_preserves_request_identity_after_an_ambiguous_response(parent_post_id):
    request_id = '89be1d5b-941f-4df0-9b67-89514b57c9cb'
    response = Mock()
    response.json.return_value = {'post': {'id': 'post-1'}}
    client = CommunityClient(api_key='test-community-key')
    with patch('lumibot.components.community.requests.post', side_effect=[TimeoutError, response]) as post:
        with pytest.raises(TimeoutError):
            client.post('Share this observation.', parent_post_id=parent_post_id, request_id=request_id)
        assert post.call_count == 1
        assert client.post('Share this observation.', parent_post_id=parent_post_id,
                           request_id=request_id) == {'id': 'post-1'}
        assert post.call_count == 2
        first, second = [call.kwargs['json'] for call in post.call_args_list]
        assert first == second
        assert first['requestId'] == request_id
        assert first.get('parentPostId') == parent_post_id


def test_legacy_call_does_not_invent_a_retry_identity():
    response = Mock()
    response.json.return_value = {'post': {'id': 'post-1'}}
    with patch('lumibot.components.community.requests.post', return_value=response) as post:
        CommunityClient(api_key='test-community-key').post('An observation.')
        assert post.call_args.kwargs['json'] == {'body': 'An observation.'}


@pytest.mark.parametrize('request_id', ['', 'not-a-uuid', '89be1d5b941f4df09b6789514b57c9cb', 123])
def test_invalid_request_identity_is_rejected_before_publication(request_id):
    with patch('lumibot.components.community.requests.post') as post:
        with pytest.raises(ValueError, match='request_id'):
            CommunityClient(api_key='test-community-key').post('An observation.', request_id=request_id)
        post.assert_not_called()
