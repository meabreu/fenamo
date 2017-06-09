CREATE OR REPLACE FUNCTION fenamosq.alta_employee(
      --ENTRADA
      pi_first_name       IN varchar,
      pi_last_name       IN varchar,
      pi_documents       IN varchar,
      pi_date_born       IN timestamp,
      pi_cellphone        IN varchar,
      pi_homephone      IN varchar,
      pi_email          IN varchar,
      pi_id_google      IN varchar,
      pi_url_img         IN varchar,
      pi_cod_user        in varchar,
      pi_cod_role        in varchar,
      pi_cod_area        in varchar,
      pi_password        in varchar,
      --TRANSACIONALES ENTRADA
      pi_master          in varchar ,
      pi_tran_date       IN timestamp,
      pi_tran_id         IN numeric  ,
      pi_tran_user       IN VARCHAR,
      --SALIDAS
      po_tran_id         OUT numeric,
      po_cod_err         OUT varchar,
      po_msj_err         OUT VARCHAR,
      po_msj_err_pl      OUT VARCHAR
  )
  RETURNS record AS
  $BODY$
  DECLARE
      c_cod_transaction  varchar(200):='ALTA_EMP_EMPLOYE';
      v_tran_id          NUMERIC;
      v_tran_user        VARCHAR(50);
      v_tran_date        TIMESTAMP;
      c_falso            varchar(2):='0';  
      v_aplica           varchar(2):='1';
      c_aplica           varchar(2):='1';
      v_id_person         NUMERIC;

  BEGIN 
       
      --Obtener valores transacccionales
      IF pi_master = c_aplica THEN 
          
          SELECT rs.po_tran_id,
                 rs.po_tran_date,
                 rs.po_cod_err,
                 rs.po_msj_err,
                 rs.po_msj_err_pl
           INTO  v_tran_id,
                 v_tran_date,
                 po_cod_err,
                 po_msj_err,
                 po_msj_err_pl
           FROM fenamosq.get_transaction(
                pi_cod_transaction    => c_cod_transaction,
                pi_tran_user          => pi_tran_user
           ) rs;

          IF po_cod_err <> 'OK' THEN 
              RAISE EXCEPTION 'TRANSFER';
          END IF;

      ELSIF pi_tran_date IS NULL OR  pi_tran_id IS NULL  OR pi_tran_user  IS NULL THEN
          po_cod_err := 'EMP_ALTA-001';
          RAISE EXCEPTION 'CONDICTION';
      ELSE 
          v_tran_id := pi_tran_id;
          v_tran_date  := pi_tran_date;
      END IF; 

      SELECT rs.po_aplica 
        INTO v_aplica 
        FROM fenamosq.validate_transaction(
              pi_cod_transaction => c_cod_transaction,
              pi_tran_user        => pi_tran_user
            ) rs;

      IF v_aplica = '0' THEN 
          po_cod_err := 'EMP_ALTA-002';
          RAISE EXCEPTION 'CONDICTION';
      ELSIF v_aplica = '2' THEN 
          po_cod_err := 'EMP_ALTA-003';
          RAISE EXCEPTION 'CONDICTION';
      END IF;

      v_id_person := nextval('fenamosq.emp_person_id_person_seq');

      po_cod_err := 'EMP_ALTA-004';
      INSERT INTO fenamosq.emp_person(
            id_person, first_name, last_name, documents, date_born, cellphone, 
            homephone, email, id_google, url_img, date_created, state, tran_date, 
            tran_id, tran_user)
       VALUES (v_id_person, pi_first_name, pi_last_name, pi_documents , pi_date_born, pi_cellphone,
               pi_homephone, pi_email, pi_id_google, pi_url_img, now(), 'A', v_tran_date,
               v_tran_id, pi_tran_user);


      po_cod_err := 'EMP_ALTA-005';
      INSERT INTO fenamosq.emp_person_hist(
            id_person, first_name, last_name, documents, date_born, cellphone, 
            homephone, email, id_google, url_img, date_created, state, tran_date_start, 
            tran_id_start, tran_user_start)
       VALUES (v_id_person, pi_first_name, pi_last_name, pi_documents , pi_date_born, pi_cellphone,
               pi_homephone, pi_email, pi_id_google, pi_url_img, now(), 'A', v_tran_date,
               v_tran_id, pi_tran_user);

      po_cod_err := 'EMP_ALTA-006';
        SELECT rs.po_tran_id,
               rs.po_cod_err,
               rs.po_msj_err,
               rs.po_msj_err_pl
          INTO po_tran_id,
               po_cod_err,
               po_msj_err,
               po_msj_err_pl
          FROM fenamosq.alta_emp_user(
            pi_cod_user  =>   pi_cod_user,
            pi_id_person =>   v_id_person,
            pi_password  =>   pi_password,
            pi_master    =>   c_falso,
            pi_tran_date =>   v_tran_date,
            pi_tran_id   =>   v_tran_id,
            pi_tran_user =>   pi_tran_user
        ) rs;

        IF po_cod_err <> 'OK' THEN 
            RAISE EXCEPTION 'TRANSFER';
        END IF;

        po_cod_err := 'EMP_ALTA-007';
        SELECT rs.po_tran_id,
               rs.po_cod_err,
               rs.po_msj_err,
               rs.po_msj_err_pl
          INTO po_tran_id,
               po_cod_err,
               po_msj_err,
               po_msj_err_pl
          FROM fenamosq.alta_emp_role_user_(
              pi_cod_user  =>   pi_cod_user,
              pi_cod_role  =>   pi_cod_role,
              pi_master    =>   c_falso,
              pi_tran_date =>   v_tran_date,
              pi_tran_id   =>   v_tran_id,
              pi_tran_user =>   pi_tran_user
          ) rs ;

        IF po_cod_err <> 'OK' THEN 
            RAISE EXCEPTION 'TRANSFER';
        END IF;

        po_cod_err := 'EMP_ALTA-008'; 
        SELECT rs.po_tran_id,
               rs.po_cod_err,
               rs.po_msj_err,
               rs.po_msj_err_pl
          INTO po_tran_id,
               po_cod_err,
               po_msj_err,
               po_msj_err_pl
          FROM fenamosq.alta_emp_area_user(
              pi_cod_area  =>   pi_cod_area,
              pi_cod_user  =>   pi_cod_user,
              pi_master    =>   c_falso,
              pi_tran_date =>   v_tran_date,
              pi_tran_id   =>   v_tran_id,
              pi_tran_user =>   pi_tran_user
          )rs ;

        IF po_cod_err <> 'OK' THEN 
            RAISE EXCEPTION 'TRANSFER';
        END IF;
          
      po_cod_err := 'OK';
      po_tran_id := v_tran_id;
  EXCEPTION
   WHEN OTHERS THEN 
        po_tran_id := v_tran_id;

        IF(SQLERRM <> 'TRANSFER') THEN 
            po_msj_err_pl = SQLERRM;
        END IF;

        IF pi_master = c_aplica THEN
          
          SELECT rs.po_msj_err
            into po_msj_err 
            FROM fenamosq.set_transaction_log(
                pi_tran_id         => v_tran_id,
                pi_tran_cod        => c_cod_transaction,
                pi_tran_date       => v_tran_date,    
                pi_tran_user       => pi_tran_user,
                pi_cod_err         => po_cod_err,
                pi_msj_err_pl      => po_msj_err_pl) rs;
        END IF;


  END; 
  $BODY$
    LANGUAGE plpgsql VOLATILE;



 ALTER FUNCTION fenamosq.alta_employee(character varying, character varying, character varying, timestamp without time zone, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, timestamp without time zone, numeric, character varying)
  OWNER TO fenamo;
