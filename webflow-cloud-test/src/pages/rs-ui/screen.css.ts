import css from '../../assets/rs-ui/screen.css?raw';
export function GET(){return new Response(css,{headers:{'Content-Type':'text/css; charset=utf-8','Cache-Control':'no-store','Access-Control-Allow-Origin':'*'}});}
