"use strict";(self.webpackChunkhudi=self.webpackChunkhudi||[]).push([[67220],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>h});var a=n(67294);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function r(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?r(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):r(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,a,i=function(e,t){if(null==e)return{};var n,a,i={},r=Object.keys(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var s=a.createContext({}),d=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},p=function(e){var t=d(e.components);return a.createElement(s.Provider,{value:t},e.children)},c="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,i=e.mdxType,r=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),c=d(n),m=i,h=c["".concat(s,".").concat(m)]||c[m]||u[m]||r;return n?a.createElement(h,o(o({ref:t},p),{},{components:n})):a.createElement(h,o({ref:t},p))}));function h(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var r=n.length,o=new Array(r);o[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[c]="string"==typeof e?e:i,o[1]=l;for(var d=2;d<r;d++)o[d]=n[d];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},58215:(e,t,n)=>{n.d(t,{Z:()=>i});var a=n(67294);const i=function(e){let{children:t,hidden:n,className:i}=e;return a.createElement("div",{role:"tabpanel",hidden:n,className:i},t)}},26396:(e,t,n)=>{n.d(t,{Z:()=>u});var a=n(87462),i=n(67294),r=n(72389),o=n(79443);const l=function(){const e=(0,i.useContext)(o.Z);if(null==e)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return e};var s=n(53810),d=n(86010);const p={tabItem:"tabItem_vU9c"};function c(e){const{lazy:t,block:n,defaultValue:r,values:o,groupId:c,className:u}=e,m=i.Children.map(e.children,(e=>{if((0,i.isValidElement)(e)&&void 0!==e.props.value)return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)})),h=o??m.map((e=>{let{props:{value:t,label:n,attributes:a}}=e;return{value:t,label:n,attributes:a}})),g=(0,s.lx)(h,((e,t)=>e.value===t.value));if(g.length>0)throw new Error(`Docusaurus error: Duplicate values "${g.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`);const v=null===r?r:r??m.find((e=>e.props.default))?.props.value??m[0]?.props.value;if(null!==v&&!h.some((e=>e.value===v)))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${v}" but none of its children has the corresponding value. Available values are: ${h.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);const{tabGroupChoices:b,setTabGroupChoices:k}=l(),[E,f]=(0,i.useState)(v),N=[],{blockElementScrollPositionUntilNextRender:w}=(0,s.o5)();if(null!=c){const e=b[c];null!=e&&e!==E&&h.some((t=>t.value===e))&&f(e)}const T=e=>{const t=e.currentTarget,n=N.indexOf(t),a=h[n].value;a!==E&&(w(t),f(a),null!=c&&k(c,a))},_=e=>{let t=null;switch(e.key){case"ArrowRight":{const n=N.indexOf(e.currentTarget)+1;t=N[n]||N[0];break}case"ArrowLeft":{const n=N.indexOf(e.currentTarget)-1;t=N[n]||N[N.length-1];break}}t?.focus()};return i.createElement("div",{className:"tabs-container"},i.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,d.Z)("tabs",{"tabs--block":n},u)},h.map((e=>{let{value:t,label:n,attributes:r}=e;return i.createElement("li",(0,a.Z)({role:"tab",tabIndex:E===t?0:-1,"aria-selected":E===t,key:t,ref:e=>N.push(e),onKeyDown:_,onFocus:T,onClick:T},r,{className:(0,d.Z)("tabs__item",p.tabItem,r?.className,{"tabs__item--active":E===t})}),n??t)}))),t?(0,i.cloneElement)(m.filter((e=>e.props.value===E))[0],{className:"margin-vert--md"}):i.createElement("div",{className:"margin-vert--md"},m.map(((e,t)=>(0,i.cloneElement)(e,{key:t,hidden:e.props.value!==E})))))}function u(e){const t=(0,r.Z)();return i.createElement(c,(0,a.Z)({key:String(t)},e))}},72493:(e,t,n)=>{n.r(t),n.d(t,{contentTitle:()=>o,default:()=>c,frontMatter:()=>r,metadata:()=>l,toc:()=>s});var a=n(87462),i=(n(67294),n(3905));n(26396),n(58215);const r={title:"SQL DML",summary:"In this page, we go will cover details on how to use DML statements on Hudi tables.",toc:!0,last_modified_at:null},o=void 0,l={unversionedId:"sql_dml",id:"version-0.14.0/sql_dml",title:"SQL DML",description:"Spark SQL",source:"@site/versioned_docs/version-0.14.0/sql_dml.md",sourceDirName:".",slug:"/sql_dml",permalink:"/docs/0.14.0/sql_dml",editUrl:"https://github.com/apache/hudi/tree/asf-site/website/versioned_docs/version-0.14.0/sql_dml.md",tags:[],version:"0.14.0",frontMatter:{title:"SQL DML",summary:"In this page, we go will cover details on how to use DML statements on Hudi tables.",toc:!0,last_modified_at:null},sidebar:"docs",previous:{title:"SQL DDL",permalink:"/docs/0.14.0/sql_ddl"},next:{title:"SQL Queries",permalink:"/docs/0.14.0/sql_queries"}},s=[{value:"Spark SQL",id:"spark-sql",children:[{value:"Insert Into",id:"insert-into",children:[],level:3},{value:"Insert Overwrite",id:"insert-overwrite",children:[],level:3},{value:"Update",id:"update",children:[],level:3},{value:"Merge Into",id:"merge-into",children:[],level:3},{value:"Delete Data",id:"delete-data",children:[],level:3},{value:"Data Skipping and Indexing",id:"data-skipping-and-indexing",children:[],level:3}],level:2},{value:"Flink",id:"flink",children:[],level:2}],d={toc:s},p="wrapper";function c(e){let{components:t,...n}=e;return(0,i.kt)(p,(0,a.Z)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h2",{id:"spark-sql"},"Spark SQL"),(0,i.kt)("p",null,"SparkSQL provides several Data Manipulation Language (DML) actions for interacting with Hudi tables. These operations allow you to insert, update, merge and delete data\nfrom your Hudi tables. Let's explore them one by one."),(0,i.kt)("p",null,"Please refer to ",(0,i.kt)("a",{parentName:"p",href:"/docs/sql_ddl"},"SQL DDL")," for creating Hudi tables using SQL."),(0,i.kt)("h3",{id:"insert-into"},"Insert Into"),(0,i.kt)("p",null,"You can use the ",(0,i.kt)("inlineCode",{parentName:"p"},"INSERT INTO")," statement to add data to a Hudi table using Spark SQL. Here are some examples:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"INSERT INTO <table> \nSELECT <columns> FROM <source>;\n")),(0,i.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"Deprecations")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"From 0.14.0, ",(0,i.kt)("inlineCode",{parentName:"p"},"hoodie.sql.bulk.insert.enable")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"hoodie.sql.insert.mode")," are deprecated. Users are expected to use ",(0,i.kt)("inlineCode",{parentName:"p"},"hoodie.spark.sql.insert.into.operation")," instead.\nTo manage duplicates with ",(0,i.kt)("inlineCode",{parentName:"p"},"INSERT INTO"),", please check out ",(0,i.kt)("a",{parentName:"p",href:"/docs/configurations#hoodiedatasourceinsertduppolicy"},"insert dup policy config"),"."))),(0,i.kt)("p",null,"Examples: "),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"-- Insert into a copy-on-write (COW) Hudi table\nINSERT INTO hudi_cow_nonpcf_tbl SELECT 1, 'a1', 20;\n\n-- Insert into a merge-on-read (MOR) Hudi table\nINSERT INTO hudi_mor_tbl SELECT 1, 'a1', 20, 1000;\n\n-- Insert into a COW Hudi table with static partition\nINSERT INTO hudi_cow_pt_tbl PARTITION(dt = '2021-12-09', hh='11') SELECT 2, 'a2', 1000;\n\n-- Insert into a COW Hudi table with dynamic partition\nINSERT INTO hudi_cow_pt_tbl PARTITION(dt, hh) SELECT 1 AS id, 'a1' AS name, 1000 AS ts, '2021-12-09' AS dt, '10' AS hh;\n")),(0,i.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"Mapping to write operations")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"Hudi offers flexibility in choosing the underlying ",(0,i.kt)("a",{parentName:"p",href:"/docs/write_operations"},"write operation")," of a ",(0,i.kt)("inlineCode",{parentName:"p"},"INSERT INTO")," statement using\nthe ",(0,i.kt)("inlineCode",{parentName:"p"},"hoodie.spark.sql.insert.into.operation")," configuration. Possible options include ",(0,i.kt)("em",{parentName:"p"},'"bulk_insert"')," (large inserts), ",(0,i.kt)("em",{parentName:"p"},'"insert"')," (with small file management),\nand ",(0,i.kt)("em",{parentName:"p"},'"upsert"')," (with deduplication/merging). If a precombine field is not set, ",(0,i.kt)("em",{parentName:"p"},'"insert"')," is chosen as the default. For a table with preCombine field set,\n",(0,i.kt)("em",{parentName:"p"},'"upsert"')," is chosen as the default operation."))),(0,i.kt)("h3",{id:"insert-overwrite"},"Insert Overwrite"),(0,i.kt)("p",null,"The ",(0,i.kt)("inlineCode",{parentName:"p"},"INSERT OVERWRITE")," statement is used to replace existing data in a Hudi table. "),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"INSERT OVERWRITE <table> \nSELECT <columns> FROM <source>;\n")),(0,i.kt)("p",null,"All existing partitions that are affected by the ",(0,i.kt)("inlineCode",{parentName:"p"},"INSERT OVERWRITE")," statement will replaced with the source data.\nHere are some examples:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"-- Overwrite non-partitioned table\nINSERT OVERWRITE hudi_mor_tbl SELECT 99, 'a99', 20.0, 900;\nINSERT OVERWRITE hudi_cow_nonpcf_tbl SELECT 99, 'a99', 20.0;\n\n-- Overwrite partitioned table with dynamic partition\nINSERT OVERWRITE TABLE hudi_cow_pt_tbl SELECT 10, 'a10', 1100, '2021-12-09', '10';\n\n-- Overwrite partitioned table with static partition\nINSERT OVERWRITE hudi_cow_pt_tbl PARTITION(dt = '2021-12-09', hh='12') SELECT 13, 'a13', 1100;\n")),(0,i.kt)("h3",{id:"update"},"Update"),(0,i.kt)("p",null,"You can use the ",(0,i.kt)("inlineCode",{parentName:"p"},"UPDATE")," statement to modify existing data in a Hudi table directly. "),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"UPDATE tableIdentifier SET column = EXPRESSION(,column = EXPRESSION) [ WHERE boolExpression]\n")),(0,i.kt)("p",null,"Here's an example:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"-- Update data in a Hudi table\nUPDATE hudi_mor_tbl SET price = price * 2, ts = 1111 WHERE id = 1;\n\n-- Update data in a partitioned Hudi table\nUPDATE hudi_cow_pt_tbl SET name = 'a1_1', ts = 1001 WHERE id = 1;\n\n-- update using non-PK field\nupdate hudi_cow_pt_tbl set ts = 1001 where name = 'a1';\n")),(0,i.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"The ",(0,i.kt)("inlineCode",{parentName:"p"},"UPDATE")," operation requires the specification of a ",(0,i.kt)("inlineCode",{parentName:"p"},"preCombineField"),"."))),(0,i.kt)("h3",{id:"merge-into"},"Merge Into"),(0,i.kt)("p",null,"The ",(0,i.kt)("inlineCode",{parentName:"p"},"MERGE INTO")," statement allows you to perform more complex updates and merges against source data. The ",(0,i.kt)("inlineCode",{parentName:"p"},"MERGE INTO")," statement\nis similar to the ",(0,i.kt)("inlineCode",{parentName:"p"},"UPDATE")," statement, but it allows you to specify different actions for matched and unmatched records."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"MERGE INTO tableIdentifier AS target_alias\nUSING (sub_query | tableIdentifier) AS source_alias\nON <merge_condition>\n[ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]\n[ WHEN NOT MATCHED [ AND <condition> ]  THEN <not_matched_action> ]\n\n<merge_condition> =A equal bool condition \n<matched_action>  =\n  DELETE  |\n  UPDATE SET *  |\n  UPDATE SET column1 = expression1 [, column2 = expression2 ...]\n<not_matched_action>  =\n  INSERT *  |\n  INSERT (column1 [, column2 ...]) VALUES (value1 [, value2 ...])\n")),(0,i.kt)("p",null,"Examples below"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"-- source table using hudi for testing merging into non-partitioned table\ncreate table merge_source (id int, name string, price double, ts bigint) using hudi\ntblproperties (primaryKey = 'id', preCombineField = 'ts');\ninsert into merge_source values (1, \"old_a1\", 22.22, 900), (2, \"new_a2\", 33.33, 2000), (3, \"new_a3\", 44.44, 2000);\n\nmerge into hudi_mor_tbl as target\nusing merge_source as source\non target.id = source.id\nwhen matched then update set *\nwhen not matched then insert *\n;\n\n-- source table using parquet for testing merging into partitioned table\ncreate table merge_source2 (id int, name string, flag string, dt string, hh string) using parquet;\ninsert into merge_source2 values (1, \"new_a1\", 'update', '2021-12-09', '10'), (2, \"new_a2\", 'delete', '2021-12-09', '11'), (3, \"new_a3\", 'insert', '2021-12-09', '12');\n\nMERGE into hudi_cow_pt_tbl as target\nusing (\n  select id, name, '1000' as ts, flag, dt, hh from merge_source2\n) source\non target.id = source.id\nwhen matched and flag != 'delete' then\n update set id = source.id, name = source.name, ts = source.ts, dt = source.dt, hh = source.hh\nwhen matched and flag = 'delete' then delete\nwhen not matched then\n insert (id, name, ts, dt, hh) values(source.id, source.name, source.ts, source.dt, source.hh)\n;\n\n")),(0,i.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"Key requirements")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"For a Hudi table with user configured primary keys, the join condition in ",(0,i.kt)("inlineCode",{parentName:"p"},"Merge Into")," is expected to contain the primary keys of the table.\nFor a Table where Hudi auto generates primary keys, the join condition in MIT can be on any arbitrary data columns."))),(0,i.kt)("h3",{id:"delete-data"},"Delete Data"),(0,i.kt)("p",null,"You can remove data from a Hudi table using the ",(0,i.kt)("inlineCode",{parentName:"p"},"DELETE FROM")," statement."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"DELETE FROM tableIdentifier [ WHERE boolExpression ]\n")),(0,i.kt)("p",null,"Examples below"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"-- Delete data from a Hudi table\nDELETE FROM hudi_cow_nonpcf_tbl WHERE uuid = 1;\n\n-- Delete data from a MOR Hudi table based on a condition\nDELETE FROM hudi_mor_tbl WHERE id % 2 = 0;\n\n-- Delete data using a non-primary key field\nDELETE FROM hudi_cow_pt_tbl WHERE name = 'a1';\n")),(0,i.kt)("h3",{id:"data-skipping-and-indexing"},"Data Skipping and Indexing"),(0,i.kt)("p",null,"DML operations can be sped up using column statistics for data skipping and using indexes to reduce the amount of data scanned.\nFor e.g. the following helps speed up the ",(0,i.kt)("inlineCode",{parentName:"p"},"DELETE")," operation on a Hudi table, by using the record level index."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"SET hoodie.enable.data.skipping=true;\nSET hoodie.metadata.record.index.enable=true;\nSET hoodie.metadata.enable=true;\n\nDELETE from hudi_table where uuid = 'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa';\n")),(0,i.kt)("p",null,"These DML operations give you powerful tools for managing your tables using Spark SQL.\nYou can control the behavior of these operations using various configuration options, as explained in the documentation."),(0,i.kt)("h2",{id:"flink"},"Flink"),(0,i.kt)("p",null,"Flink SQL also provides several Data Manipulation Language (DML) actions for interacting with Hudi tables. All these operations are already\nshowcased in the ",(0,i.kt)("a",{parentName:"p",href:"/docs/flink-quick-start-guide"},"Flink Quickstart"),"."))}c.isMDXComponent=!0}}]);