CREATE OR REPLACE FUNCTION fenamosq.alta_cli_person(
      pi_first_name      IN varchar,
      pi_last_name       IN varchar,
      pi_email           in varchar ,
      pi_documents       IN varchar,
      pi_cellphone       IN varchar  ,
      pi_homephone       IN varchar,
      pi_compania        IN VARCHAR,
      pi_rnc             IN VARCHAR,
      pi_master          in varchar ,
      pi_tran_date       IN timestamp,
      pi_tran_id         IN numeric  ,
      pi_tran_user       IN VARCHAR,
      po_id_client       OUT NUMERIC,
      po_tran_id         OUT numeric,
      po_cod_err         OUT varchar,
      po_msj_err         OUT VARCHAR,
      po_msj_err_pl      OUT VARCHAR
  )

  RETURNS record AS
  $BODY$
  DECLARE
      c_cod_transaction  varchar(200):='ALTA_CLI_PERSON';
      v_tran_id      NUMERIC;
      v_tran_user  VARCHAR(50);
      v_tran_date  TIMESTAMP;      
      v_validar  varchar(20);
      c_falso    varchar(20):='0';
      v_aplica    varchar(2):='1';
      c_aplica    varchar(2):='1';
      v_id_client  NUMERIC;
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
          po_cod_err := 'EMP-ROL-001';
          RAISE EXCEPTION 'EXCEPTION';
      ELSE 
          v_tran_id := pi_tran_id;
          v_tran_date  := pi_tran_date;
      END IF; 

      SELECT rs.po_aplica 
        INTO v_aplica 
        FROM fenamosq.validate_transaction(
              pi_cod_transaction => c_cod_transaction,
              pi_tran_user        => v_tran_user
            ) rs;

      IF v_aplica = '0' THEN 
          po_cod_err := 'CLI-ALTA-002';
          RAISE EXCEPTION 'CONDICTION';
      ELSIF v_aplica = '2' THEN 
          po_cod_err := 'CLI-ALTA-003';
          RAISE EXCEPTION 'CONDICTION';
      END IF;

      v_id_client   := nextval('fenamosq.cli_person_id_client_seq');
      po_cod_err := 'CLI-ALTA-004';
      INSERT INTO fenamosq.cli_person(
            id_client, first_name, last_name, documents, date_born, cellphone, homephone, compania, rnc, email, 
            id_google, url_img, date_created, state, tran_date, tran_id, tran_user)
    VALUES (v_id_client, pi_first_name, pi_last_name, pi_documents, null, pi_cellphone,pi_homephone,pi_compania, pi_rnc, pi_email,
            null, null, now(), 'A', v_tran_date, v_tran_id, pi_tran_user);

    INSERT INTO fenamosq.cli_person_hist(
            id_client, first_name, last_name, documents, date_born, cellphone, homephone, compania, rnc, email, 
            id_google, url_img, date_created, state, tran_date_start, tran_id_start, tran_user_start)
    VALUES (v_id_client, pi_first_name, pi_last_name, pi_documents, null, pi_cellphone,pi_homephone,pi_compania, pi_rnc, pi_email ,
            null, null, now(), 'A', v_tran_date, v_tran_id, pi_tran_user);

      po_id_client := v_id_client;
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

ALTER FUNCTION fenamosq.alta_cli_person(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, timestamp without time zone, numeric, character varying)
  OWNER TO fenamo;
