"""Run with: python examples/quickstart.py

Uses the offline mock model so the example does not download weights or call Jev.
"""

from keziah import Keziah

QUESTIONS = {
    "frustration": {
        "type": "score",
        "instructions": "Rate the customer's frustration.",
        "criteria": ["calm", "frustrated", "very angry"],
    },
    "needs_human": {
        "type": "noul",
        "instructions": "Does this require human intervention?",
    },
}


def main() -> None:
    with Keziah(mode="memory") as keziah:
        job_id = keziah.submit(
            model="mock",
            state={"message": "I've asked three times and nobody has fixed it."},
            questions=QUESTIONS,
        )
        print(keziah.wait(job_id))

        batch = keziah.submit_batch(
            [
                {"state": {"message": "Message one"}, "questions": QUESTIONS},
                {"state": {"message": "Message two"}, "questions": QUESTIONS},
            ],
            model="mock",
        )
        for result in keziah.wait_batch(batch.batch_id):
            print(result.batch_ordinal, result.status, result.response)


if __name__ == "__main__":
    main()
