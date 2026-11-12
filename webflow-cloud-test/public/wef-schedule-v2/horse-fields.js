export const HORSE_GENDERS=['Gelding','Mare'];
export const HORSE_COLORS=['Bay','Grey','Chestnut','Liverchestnut','Black','Palomino','Paint'];
export function validHorseFields(fields){
 return fields&&typeof fields==='object'&&!Array.isArray(fields)&&Object.keys(fields).length>0&&Object.keys(fields).every(k=>['nickname','color','gender'].includes(k))&&Object.entries(fields).every(([k,v])=>typeof v==='string'&&(k==='nickname'?v.length<=80:k==='color'?HORSE_COLORS.includes(v)||v==='':HORSE_GENDERS.includes(v)||v===''));
}
