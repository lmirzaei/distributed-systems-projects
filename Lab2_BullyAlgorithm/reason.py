from enum import Enum


class Reason(Enum):
    """
    Enumeration of reasons for which a socket begins sending or waiting for a message
    """
    # For the following reasons, the process intends to begin an election:
    JUST_JOINED = 'JUST JOIN GROUP'  # When I want to start an election on startup.
    TIMEOUT_NO_COORDINATOR_RECEIVED = 'COORDINATOR TIMEOUT OCCURRED'  # When I'm waiting to see who is the winner,
    # but I don't get before the timeout.
    GET_ELECTION_MESSAGE = 'A PEER SENT ELECTION TO ME'  # When a peer/a member sent me an ELECTION message

    # For the following reasons, the process intends to declare victory:
    TIMEOUT_NO_OK_RECEIVED = 'OK TIMEOUT OCCURRED'  # When the I reached timeout while waiting for peers to
    # send OK
    COORDINATOR_MYSELF = 'I HAVE HIGHEST PID'  # When ELECTION is finished and I'm the member with highest pid
