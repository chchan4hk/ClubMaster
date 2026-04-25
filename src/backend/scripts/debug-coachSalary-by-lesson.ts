/**
 * Debug helper: prints CoachManager (coach salary) rows for one club + lesson.
 *
 * From `src/backend`:
 *   node ./node_modules/tsx/dist/cli.mjs ./scripts/debug-coachSalary-by-lesson.ts CM00000008 LE0000004
 */
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import { closeMongoClient, getCoachSalaryCollection, resolveCoachSalaryDatabaseName } from "../src/db/DBConnection";

const backendRoot = path.join(__dirname, "..");
loadLocalEnvFile(backendRoot);

async function main(): Promise<void> {
  const clubId = String(process.argv[2] ?? "").trim();
  const lessonId = String(process.argv[3] ?? "").trim();
  if (!clubId) {
    console.error("Usage: <clubId> [lessonId] (e.g. CM00000008 LE0000004)");
    process.exit(1);
  }
  const dbName = resolveCoachSalaryDatabaseName();
  const col = await getCoachSalaryCollection();
  const clubScope = {
    $or: [
      { ClubID: clubId },
      { club_id: clubId } as any,
      { coach_manager_uid: clubId } as any,
    ],
  };
  const filter = lessonId
    ? ({
        $and: [
          clubScope,
          {
            lessonId: new RegExp(
              `^${lessonId.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`,
              "i",
            ),
          } as any,
        ],
      } as any)
    : (clubScope as any);

  const docs = await col.find(filter).sort({ CoachSalaryID: 1 }).toArray();

  console.log(
    `DB ${dbName} · collection CoachManager · club=${clubId}` +
      (lessonId ? ` · lesson=${lessonId}` : "") +
      ` · matches=${docs.length}`,
  );
  console.log(JSON.stringify(docs, null, 2));
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(() => {
    void closeMongoClient();
  });

