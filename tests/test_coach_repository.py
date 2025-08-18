# tests/test_coach_repository.py
from models.coach import Coach

def test_fetch_all_ids(fake_repo):
    ids = fake_repo.fetch_all_ids()
    assert 1 in ids
    assert isinstance(ids, dict)


def test_insert_new_coach(fake_repo, new_coach):
    assert new_coach.tm_coach_id not in fake_repo.fetch_all_ids()
    fake_repo.save(new_coach)
    assert new_coach.tm_coach_id in fake_repo.fetch_all_ids()


def test_insert_overwrites(fake_repo):
    c = Coach(tm_coach_id=1, name="Updated", dob="1975-01-01", country="FR", coaching_license="UEFA")
    fake_repo.save(c)
    assert fake_repo.coaches[1].name == "Updated"