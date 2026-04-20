/**
 * Main login lookups for Mongo-backed auth use the `userLogin` collection (see `userListMongo.ts`).
 */
export {
  findUserByUsernameMongo,
  findCoachManagerUidByClubNameMongo,
} from "./userListMongo";
