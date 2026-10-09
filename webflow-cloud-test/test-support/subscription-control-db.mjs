import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
export function subscriptionControlDatabase(){
 const sql=new DatabaseSync(':memory:');
 sql.exec(readFileSync(new URL('../migrations/recognize-control/0001_claims.sql',import.meta.url),'utf8'));
 sql.exec(readFileSync(new URL('../migrations/recognize-control/0002_subscriptions.sql',import.meta.url),'utf8'));
 return {sql,close:()=>sql.close(),prepare(text){return {bind(...values){return {
  async run(){return {meta:{changes:sql.prepare(text).run(...values).changes}}},
  async first(){return sql.prepare(text).get(...values)||null}
 };}};}};
}
